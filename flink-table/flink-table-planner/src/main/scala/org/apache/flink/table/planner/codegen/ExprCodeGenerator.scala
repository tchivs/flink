/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.codegen

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.util.DataFormatConverters.{getConverterForDataType, DataFormatConverter}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, RexDistinctKeyVariable, RexFieldVariable}
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.GenerateUtils._
import org.apache.flink.table.planner.codegen.JsonGenerateUtils.{isJsonFunctionOperand, isSupportedJsonOperand}
import org.apache.flink.table.planner.codegen.calls._
import org.apache.flink.table.planner.codegen.calls.ScalarOperatorGens._
import org.apache.flink.table.planner.codegen.calls.SearchOperatorGen.generateSearch
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable._
import org.apache.flink.table.planner.functions.sql.SqlThrowExceptionFunction
import org.apache.flink.table.planner.functions.utils.{ScalarSqlFunction, TableSqlFunction}
import org.apache.flink.table.planner.plan.utils.RexLiteralUtil
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isInteroperable
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils.{isNumeric, isTemporal, isTimeInterval}
import org.apache.flink.table.types.logical._
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.{ReturnTypes, SqlTypeName}
import org.apache.calcite.sql.{SqlKind, SqlOperator}

import scala.collection.JavaConversions._

/**
 * This code generator is mainly responsible for generating codes for a given calcite [[RexNode]].
 * It can also generate type conversion codes for the result converter.
 */
class ExprCodeGenerator(ctx: CodeGeneratorContext, nullableInput: Boolean)
  extends RexVisitor[GeneratedExpression] {

  /** term of the [[ProcessFunction]]'s context, can be changed when needed */
  var contextTerm = "ctx"

  /** information of the first input */
  var input1Type: LogicalType = _
  var input1Term: String = _
  var input1FieldMapping: Option[Array[Int]] = None

  /** information of the optional second input */
  var input2Type: Option[LogicalType] = None
  var input2Term: Option[String] = None
  var input2FieldMapping: Option[Array[Int]] = None

  /** information of the user-defined constructor */
  var functionContextTerm: Option[String] = None

  /** Bind the input information, should be called before generating expression. */
  def bindInput(
      inputType: LogicalType,
      inputTerm: String = DEFAULT_INPUT1_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    input1Type = inputType
    input1Term = inputTerm
    input1FieldMapping = inputFieldMapping
    this
  }

  /**
   * In some cases, the expression will have two inputs (e.g. join condition and udtf). We should
   * bind second input information before use.
   */
  def bindSecondInput(
      inputType: LogicalType,
      inputTerm: String = DEFAULT_INPUT2_TERM,
      inputFieldMapping: Option[Array[Int]] = None): ExprCodeGenerator = {
    input2Type = Some(inputType)
    input2Term = Some(inputTerm)
    input2FieldMapping = inputFieldMapping
    this
  }

  /**
   * In some cases, we should use user-defined input for constructor. For example,
   * ScalaFunctionCodeGen allows to use user-defined context term rather than get from invoking
   * getRuntimeContext() method.
   */
  def bindConstructorTerm(inputFunctionContextTerm: String): ExprCodeGenerator = {
    functionContextTerm = Some(inputFunctionContextTerm)
    this
  }

  private lazy val input1Mapping: Array[Int] = input1FieldMapping match {
    case Some(mapping) => mapping
    case _ => fieldIndices(input1Type)
  }

  private lazy val input2Mapping: Array[Int] = input2FieldMapping match {
    case Some(mapping) => mapping
    case _ =>
      input2Type match {
        case Some(input) => fieldIndices(input)
        case _ => Array[Int]()
      }
  }

  /**
   * Generates an expression from a RexNode. If objects or variables can be reused, they will be
   * added to reusable code sections internally.
   *
   * @param rex
   *   Calcite row expression
   * @return
   *   instance of GeneratedExpression
   */
  def generateExpression(rex: RexNode): GeneratedExpression = {
    rex.accept(this)
  }

  /**
   * Generates an expression that converts the first input (and second input) into the given type.
   * If two inputs are converted, the second input is appended. If objects or variables can be
   * reused, they will be added to reusable code sections internally. The evaluation result will be
   * stored in the variable outRecordTerm.
   *
   * @param returnType
   *   conversion target type. Inputs and output must have the same arity.
   * @param outRecordTerm
   *   the result term
   * @param outRecordWriterTerm
   *   the result writer term
   * @param reusedOutRow
   *   If objects or variables can be reused, they will be added to reusable code sections
   *   internally.
   * @return
   *   instance of GeneratedExpression
   */
  def generateConverterResultExpression(
      returnType: RowType,
      returnTypeClazz: Class[_ <: RowData],
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRow: Boolean = true,
      fieldCopy: Boolean = false,
      rowtimeExpression: Option[RexNode] = None): GeneratedExpression = {
    val input1AccessExprs = input1Mapping.map {
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER | TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER
          if rowtimeExpression.isDefined =>
        // generate rowtime attribute from expression
        generateExpression(rowtimeExpression.get)
      case TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER |
          TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER =>
        throw new TableException("Rowtime extraction expression missing. Please report a bug.")
      case TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER =>
        // attribute is proctime indicator.
        // we use a null literal and generate a timestamp when we need it.
        generateNullLiteral(new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3))
      case TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER =>
        // attribute is proctime field in a batch query.
        // it is initialized with the current time.
        generateCurrentTimestamp(ctx)
      case idx =>
        // get type of result field
        generateInputAccess(ctx, input1Type, input1Term, idx, nullableInput, fieldCopy)
    }

    val input2AccessExprs = input2Type match {
      case Some(ti) =>
        input2Mapping
          .map(idx => generateInputAccess(ctx, ti, input2Term.get, idx, nullableInput, true))
          .toSeq
      case None => Seq() // add nothing
    }

    generateResultExpression(
      input1AccessExprs ++ input2AccessExprs,
      returnType,
      returnTypeClazz,
      outRow = outRecordTerm,
      outRowWriter = Some(outRecordWriterTerm),
      reusedOutRow = reusedOutRow)
  }

  /**
   * Generates an expression from a sequence of other expressions. The evaluation result may be
   * stored in the variable outRecordTerm.
   *
   * @param fieldExprs
   *   field expressions to be converted
   * @param returnType
   *   conversion target type. Type must have the same arity than fieldExprs.
   * @param outRow
   *   the result term
   * @param outRowWriter
   *   the result writer term for BinaryRowData.
   * @param reusedOutRow
   *   If objects or variables can be reused, they will be added to reusable code sections
   *   internally.
   * @param outRowAlreadyExists
   *   Don't need addReusableRecord if out row already exists.
   * @return
   *   instance of GeneratedExpression
   */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      returnType: RowType,
      returnTypeClazz: Class[_ <: RowData],
      outRow: String = DEFAULT_OUT_RECORD_TERM,
      outRowWriter: Option[String] = Some(DEFAULT_OUT_RECORD_WRITER_TERM),
      reusedOutRow: Boolean = true,
      outRowAlreadyExists: Boolean = false): GeneratedExpression = {
    val fieldExprIdxToOutputRowPosMap = fieldExprs.indices.map(i => i -> i).toMap
    generateResultExpression(
      fieldExprs,
      fieldExprIdxToOutputRowPosMap,
      returnType,
      returnTypeClazz,
      outRow,
      outRowWriter,
      reusedOutRow,
      outRowAlreadyExists)
  }

  /**
   * Generates an expression from a sequence of other expressions. The evaluation result may be
   * stored in the variable outRecordTerm.
   *
   * @param fieldExprs
   *   field expressions to be converted
   * @param fieldExprIdxToOutputRowPosMap
   *   Mapping index of fieldExpr in `fieldExprs` to position of output row.
   * @param returnType
   *   conversion target type. Type must have the same arity than fieldExprs.
   * @param outRow
   *   the result term
   * @param outRowWriter
   *   the result writer term for BinaryRowData.
   * @param reusedOutRow
   *   If objects or variables can be reused, they will be added to reusable code sections
   *   internally.
   * @param outRowAlreadyExists
   *   Don't need addReusableRecord if out row already exists.
   * @return
   *   instance of GeneratedExpression
   */
  def generateResultExpression(
      fieldExprs: Seq[GeneratedExpression],
      fieldExprIdxToOutputRowPosMap: Map[Int, Int],
      returnType: RowType,
      returnTypeClazz: Class[_ <: RowData],
      outRow: String,
      outRowWriter: Option[String],
      reusedOutRow: Boolean,
      outRowAlreadyExists: Boolean): GeneratedExpression = {
    // initial type check
    if (returnType.getFieldCount != fieldExprs.length) {
      throw new CodeGenException(
        s"Arity [${returnType.getFieldCount}] of result type [$returnType] does not match " +
          s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    if (fieldExprIdxToOutputRowPosMap.size != fieldExprs.length) {
      throw new CodeGenException(
        s"Size [${returnType.getFieldCount}] of fieldExprIdxToOutputRowPosMap does not match " +
          s"number [${fieldExprs.length}] of expressions [$fieldExprs].")
    }
    // type check
    fieldExprs.zipWithIndex.foreach {
      // timestamp type(Include TimeIndicator) and generic type can compatible with each other.
      case (fieldExpr, i)
          if fieldExpr.resultType.isInstanceOf[TypeInformationRawType[_]] ||
            fieldExpr.resultType.isInstanceOf[TimestampType] =>
        if (
          returnType.getTypeAt(i).getClass != fieldExpr.resultType.getClass
          && !returnType.getTypeAt(i).isInstanceOf[TypeInformationRawType[_]]
        ) {
          throw new CodeGenException(
            s"Incompatible types of expression and result type, Expression[$fieldExpr] type is " +
              s"[${fieldExpr.resultType}], result type is [${returnType.getTypeAt(i)}]")
        }
      case (fieldExpr, i) if !isInteroperable(fieldExpr.resultType, returnType.getTypeAt(i)) =>
        throw new CodeGenException(
          s"Incompatible types of expression and result type. Expression[$fieldExpr] type is " +
            s"[${fieldExpr.resultType}], result type is [${returnType.getTypeAt(i)}]")
      case _ => // ok
    }

    val setFieldsCode = fieldExprs.zipWithIndex
      .map {
        case (fieldExpr, index) =>
          val pos = fieldExprIdxToOutputRowPosMap.getOrElse(
            index,
            throw new CodeGenException(s"Illegal field expr index: $index"))
          rowSetField(ctx, returnTypeClazz, outRow, pos.toString, fieldExpr, outRowWriter)
      }
      .mkString("\n")

    val outRowInitCode = if (!outRowAlreadyExists) {
      val initCode = generateRecordStatement(returnType, returnTypeClazz, outRow, outRowWriter, ctx)
      if (reusedOutRow) {
        NO_CODE
      } else {
        initCode
      }
    } else {
      NO_CODE
    }

    val code = if (returnTypeClazz == classOf[BinaryRowData] && outRowWriter.isDefined) {
      val writer = outRowWriter.get
      val resetWriter = s"$writer.reset();"
      val completeWriter: String = s"$writer.complete();"
      s"""
         |$outRowInitCode
         |$resetWriter
         |$setFieldsCode
         |$completeWriter
        """.stripMargin
    } else {
      s"""
         |$outRowInitCode
         |$setFieldsCode
        """.stripMargin
    }
    GeneratedExpression(outRow, NEVER_NULL, code, returnType)
  }

  override def visitInputRef(inputRef: RexInputRef): GeneratedExpression = {
    // for specific custom code generation
    if (input1Type == null) {
      return GeneratedExpression(
        inputRef.getName,
        inputRef.getName + "IsNull",
        NO_CODE,
        FlinkTypeFactory.toLogicalType(inputRef.getType))
    }
    // for the general cases with a previous call to bindInput()
    val input1Arity = input1Type match {
      case r: RowType => r.getFieldCount
      case _ => 1
    }
    // if inputRef index is within size of input1 we work with input1, input2 otherwise
    val input = if (inputRef.getIndex < input1Arity) {
      (input1Type, input1Term)
    } else {
      (
        input2Type.getOrElse(throw new CodeGenException("Invalid input access.")),
        input2Term.getOrElse(throw new CodeGenException("Invalid input access.")))
    }

    val index = if (input._2 == input1Term) {
      inputRef.getIndex
    } else {
      inputRef.getIndex - input1Arity
    }

    generateInputAccess(ctx, input._1, input._2, index, nullableInput, true)
  }

  override def visitTableInputRef(rexTableInputRef: RexTableInputRef): GeneratedExpression =
    visitInputRef(rexTableInputRef)

  override def visitFieldAccess(rexFieldAccess: RexFieldAccess): GeneratedExpression = {
    val refExpr = rexFieldAccess.getReferenceExpr.accept(this)
    val index = rexFieldAccess.getField.getIndex
    val fieldAccessExpr = generateFieldAccess(ctx, refExpr.resultType, refExpr.resultTerm, index)

    val resultType = fieldAccessExpr.resultType

    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val defaultValue = primitiveDefaultValue(resultType)
    val Seq(resultTerm, nullTerm) =
      ctx.addReusableLocalVariables((resultTypeTerm, "result"), ("boolean", "isNull"))

    val resultCode =
      s"""
         |${refExpr.code}
         |if (${refExpr.nullTerm}) {
         |  $resultTerm = $defaultValue;
         |  $nullTerm = true;
         |}
         |else {
         |  ${fieldAccessExpr.code}
         |  $resultTerm = ${fieldAccessExpr.resultTerm};
         |  $nullTerm = ${fieldAccessExpr.nullTerm};
         |}
         |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, resultCode, fieldAccessExpr.resultType)
  }

  override def visitLiteral(literal: RexLiteral): GeneratedExpression = {
    val res = RexLiteralUtil.toFlinkInternalValue(literal)
    generateLiteral(ctx, res.f0, res.f1)
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): GeneratedExpression = {
    GeneratedExpression(input1Term, NEVER_NULL, NO_CODE, input1Type)
  }

  override def visitLocalRef(localRef: RexLocalRef): GeneratedExpression =
    throw new CodeGenException("RexLocalRef are not supported yet.")

  def visitRexFieldVariable(variable: RexFieldVariable): GeneratedExpression = {
    val internalType = FlinkTypeFactory.toLogicalType(variable.dataType)
    val nullTerm = variable.fieldTerm + "IsNull" // not use newName, keep isNull unique.
    ctx.addReusableMember(s"${primitiveTypeTermForType(internalType)} ${variable.fieldTerm};")
    ctx.addReusableMember(s"boolean $nullTerm;")
    GeneratedExpression(variable.fieldTerm, nullTerm, NO_CODE, internalType)
  }

  def visitDistinctKeyVariable(value: RexDistinctKeyVariable): GeneratedExpression = {
    val inputExpr = ctx.getReusableInputUnboxingExprs(input1Term, 0) match {
      case Some(expr) => expr
      case None =>
        val pType = primitiveTypeTermForType(value.internalType)
        val defaultValue = primitiveDefaultValue(value.internalType)
        val resultTerm = newName(ctx, "field")
        val nullTerm = newName(ctx, "isNull")
        val code =
          s"""
             |$pType $resultTerm = $defaultValue;
             |boolean $nullTerm = true;
             |if ($input1Term != null) {
             |  $nullTerm = false;
             |  $resultTerm = ($pType) $input1Term;
             |}
            """.stripMargin
        val expr = GeneratedExpression(resultTerm, nullTerm, code, value.internalType)
        ctx.addReusableInputUnboxingExprs(input1Term, 0, expr)
        expr
    }
    // hide the generated code as it will be executed only once
    GeneratedExpression(inputExpr.resultTerm, inputExpr.nullTerm, NO_CODE, inputExpr.resultType)
  }

  override def visitRangeRef(rangeRef: RexRangeRef): GeneratedExpression =
    throw new CodeGenException("Range references are not supported yet.")

  override def visitDynamicParam(dynamicParam: RexDynamicParam): GeneratedExpression =
    throw new CodeGenException("Dynamic parameter references are not supported yet.")

  override def visitCall(call: RexCall): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toLogicalType(call.getType)

    // throw exception if json function is called outside JSON_OBJECT or JSON_ARRAY function
    if (isJsonFunctionOperand(call)) {
      throw new ValidationException(
        "The JSON() function is currently only supported inside JSON_ARRAY() or as the VALUE param" +
          " of JSON_OBJECT(). Example: JSON_OBJECT('a', JSON('{\"key\": \"value\"}')) or " +
          "JSON_ARRAY(JSON('{\"key\": \"value\"}')).")
    }

    if (call.getKind == SqlKind.SEARCH) {
      return generateSearch(
        ctx,
        generateExpression(call.getOperands.get(0)),
        call.getOperands.get(1).asInstanceOf[RexLiteral])
    }

    // convert operands and help giving untyped NULL literals a type
    val operands = call.getOperands.zipWithIndex.map {

      // this helps e.g. for AS(null)
      // we might need to extend this logic in case some rules do not create typed NULLs
      case (operandLiteral: RexLiteral, 0)
          if operandLiteral.getType.getSqlTypeName == SqlTypeName.NULL &&
            call.getOperator.getReturnTypeInference == ReturnTypes.ARG0 =>
        generateNullLiteral(resultType)

      // We only support the JSON function inside of JSON_OBJECT or JSON_ARRAY
      case (operand: RexNode, i) if isSupportedJsonOperand(operand, call, i) =>
        generateJsonCall(operand)

      case (o @ _, _) => o.accept(this)
    }

    generateCallExpression(ctx, call, operands, resultType)
  }

  override def visitOver(over: RexOver): GeneratedExpression =
    throw new CodeGenException("Aggregate functions over windows are not supported yet.")

  override def visitSubQuery(subQuery: RexSubQuery): GeneratedExpression =
    throw new CodeGenException("Subqueries are not supported yet.")

  override def visitPatternFieldRef(fieldRef: RexPatternFieldRef): GeneratedExpression =
    throw new CodeGenException("Pattern field references are not supported yet.")

  // ----------------------------------------------------------------------------------------

  private def generateCallExpression(
      ctx: CodeGeneratorContext,
      call: RexCall,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType): GeneratedExpression = {
    call.getOperator match {
      // arithmetic
      case PLUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "+", resultType, left, right)

      case PLUS | DATETIME_PLUS if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(ctx, plus = true, resultType, left, right)

      case MINUS if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "-", resultType, left, right)

      case MINUS | MINUS_DATE if isTemporal(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireTemporal(left)
        requireTemporal(right)
        generateTemporalPlusMinus(ctx, plus = false, resultType, left, right)

      case MULTIPLY if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "*", resultType, left, right)

      case MULTIPLY if isTimeInterval(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumericAndTimeInterval(left, right)
        generateBinaryArithmeticOperator(ctx, "*", resultType, left, right)

      case DIVIDE | DIVIDE_INTEGER if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "/", resultType, left, right)

      case MOD if isNumeric(resultType) =>
        val left = operands.head
        val right = operands(1)
        requireNumeric(left)
        requireNumeric(right)
        generateBinaryArithmeticOperator(ctx, "%", resultType, left, right)

      case UNARY_MINUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator(ctx, "-", resultType, operand)

      case UNARY_MINUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(ctx, plus = false, resultType, operand)

      case UNARY_PLUS if isNumeric(resultType) =>
        val operand = operands.head
        requireNumeric(operand)
        generateUnaryArithmeticOperator(ctx, "+", resultType, operand)

      case UNARY_PLUS if isTimeInterval(resultType) =>
        val operand = operands.head
        requireTimeInterval(operand)
        generateUnaryIntervalPlusMinus(ctx, plus = true, resultType, operand)

      // comparison
      case EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateEquals(ctx, left, right, resultType)

      case IS_DISTINCT_FROM =>
        val left = operands.head
        val right = operands(1)
        generateIsDistinctFrom(ctx, left, right, resultType)

      case IS_NOT_DISTINCT_FROM =>
        val left = operands.head
        val right = operands(1)
        generateIsNotDistinctFrom(ctx, left, right, resultType)

      case NOT_EQUALS =>
        val left = operands.head
        val right = operands(1)
        generateNotEquals(ctx, left, right, resultType)

      case GREATER_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(ctx, ">", left, right, resultType)

      case GREATER_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(ctx, ">=", left, right, resultType)

      case LESS_THAN =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(ctx, "<", left, right, resultType)

      case LESS_THAN_OR_EQUAL =>
        val left = operands.head
        val right = operands(1)
        requireComparable(left)
        requireComparable(right)
        generateComparison(ctx, "<=", left, right, resultType)

      case IS_NULL =>
        val operand = operands.head
        generateIsNull(ctx, operand, resultType)

      case IS_NOT_NULL =>
        val operand = operands.head
        generateIsNotNull(ctx, operand, resultType)

      // logic
      case AND =>
        operands.reduceLeft {
          (left: GeneratedExpression, right: GeneratedExpression) =>
            requireBoolean(left)
            requireBoolean(right)
            generateAnd(ctx, left, right, resultType)
        }

      case OR =>
        operands.reduceLeft {
          (left: GeneratedExpression, right: GeneratedExpression) =>
            requireBoolean(left)
            requireBoolean(right)
            generateOr(ctx, left, right, resultType)
        }

      case NOT =>
        val operand = operands.head
        requireBoolean(operand)
        generateNot(ctx, operand, resultType)

      case CASE =>
        generateIfElse(ctx, operands, resultType)

      case IS_TRUE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsTrue(operand, resultType)

      case IS_NOT_TRUE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsNotTrue(operand, resultType)

      case IS_FALSE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsFalse(operand, resultType)

      case IS_NOT_FALSE =>
        val operand = operands.head
        requireBoolean(operand)
        generateIsNotFalse(operand, resultType)

      // casting
      case CAST =>
        generateCast(
          ctx,
          operands.head,
          resultType,
          nullOnFailure = ctx.tableConfig
            .get(ExecutionConfigOptions.TABLE_EXEC_LEGACY_CAST_BEHAVIOUR)
            .isEnabled)

      case TRY_CAST =>
        generateCast(ctx, operands.head, resultType, nullOnFailure = true)

      // Reinterpret
      case REINTERPRET =>
        val operand = operands.head
        generateReinterpret(ctx, operand, resultType)

      // as / renaming
      case AS =>
        operands.head

      case DESCRIPTOR =>
        generateDescriptor(ctx, operands, resultType)

      // rows
      case ROW =>
        generateRow(ctx, resultType, operands)

      // arrays
      case ARRAY_VALUE_CONSTRUCTOR =>
        generateArray(ctx, resultType, operands)

      // maps
      case MAP_VALUE_CONSTRUCTOR =>
        generateMap(ctx, resultType, operands)

      case ITEM =>
        operands.head.resultType.getTypeRoot match {
          case LogicalTypeRoot.ARRAY =>
            val array = operands.head
            val index = operands(1)
            requireInteger(index)
            generateArrayElementAt(ctx, array, index)

          case LogicalTypeRoot.MAP =>
            val key = operands(1)
            generateMapGet(ctx, operands.head, key)

          case LogicalTypeRoot.ROW | LogicalTypeRoot.STRUCTURED_TYPE =>
            generateDot(ctx, operands)

          case _ => throw new CodeGenException("Expect an array, a map or a row.")
        }

      case CARDINALITY =>
        operands.head.resultType match {
          case t: LogicalType if TypeCheckUtils.isArray(t) =>
            val array = operands.head
            generateArrayCardinality(ctx, array, resultType)

          case t: LogicalType if TypeCheckUtils.isMap(t) =>
            val map = operands.head
            generateMapCardinality(ctx, map, resultType)

          case _ => throw new CodeGenException("Expect an array or a map.")
        }

      case ELEMENT =>
        val array = operands.head
        requireArray(array)
        generateArrayElement(ctx, array)

      case DOT =>
        generateDot(ctx, operands)

      case PROCTIME =>
        // attribute is proctime indicator.
        // We use a null literal and generate a timestamp when we need it.
        generateNullLiteral(new LocalZonedTimestampType(true, TimestampKind.PROCTIME, 3))

      case PROCTIME_MATERIALIZE =>
        generateProctimeTimestamp(ctx, contextTerm)

      case STREAMRECORD_TIMESTAMP =>
        generateRowtimeAccess(ctx, contextTerm, false)

      case JSON_VALUE => new JsonValueCallGen().generate(ctx, operands, resultType)

      case JSON_QUERY => new JsonQueryCallGen().generate(ctx, operands, resultType)

      case JSON_OBJECT => new JsonObjectCallGen(call).generate(ctx, operands, resultType)

      case JSON_ARRAY => new JsonArrayCallGen(call).generate(ctx, operands, resultType)

      case _: SqlThrowExceptionFunction =>
        val nullValue = generateNullLiteral(resultType)
        val code =
          s"""
             |${operands.map(_.code).mkString("\n")}
             |${nullValue.code}
             |org.apache.flink.util.ExceptionUtils.rethrow(
             |  new org.apache.flink.table.api.TableRuntimeException(${operands.head.resultTerm}.toString()));
             |""".stripMargin
        GeneratedExpression(nullValue.resultTerm, nullValue.nullTerm, code, resultType)

      case ssf: ScalarSqlFunction =>
        new ScalarFunctionCallGen(
          ssf.makeFunction(getOperandLiterals(operands), operands.map(_.resultType).toArray))
          .generate(ctx, operands, resultType)

      case tsf: TableSqlFunction =>
        new TableFunctionCallGen(
          call,
          tsf.makeFunction(getOperandLiterals(operands), operands.map(_.resultType).toArray))
          .generate(ctx, operands, resultType)

      case bsf: BridgingSqlFunction =>
        bsf.getDefinition match {

          case BuiltInFunctionDefinitions.CURRENT_WATERMARK =>
            generateWatermark(ctx, contextTerm, resultType)

          case BuiltInFunctionDefinitions.GREATEST =>
            operands.foreach(operand => requireComparable(operand))
            generateGreatestLeast(ctx, resultType, operands)

          case BuiltInFunctionDefinitions.LEAST =>
            operands.foreach(operand => requireComparable(operand))
            generateGreatestLeast(ctx, resultType, operands, greatest = false)

          case BuiltInFunctionDefinitions.JSON_STRING =>
            new JsonStringCallGen(call).generate(ctx, operands, resultType)

          case BuiltInFunctionDefinitions.INTERNAL_HASHCODE =>
            new HashCodeCallGen().generate(ctx, operands, resultType)

          case BuiltInFunctionDefinitions.AGG_DECIMAL_PLUS |
              BuiltInFunctionDefinitions.HIVE_AGG_DECIMAL_PLUS =>
            val left = operands.head
            val right = operands(1)
            generateBinaryArithmeticOperator(ctx, "+", resultType, left, right)

          case BuiltInFunctionDefinitions.AGG_DECIMAL_MINUS =>
            val left = operands.head
            val right = operands(1)
            generateBinaryArithmeticOperator(ctx, "-", resultType, left, right)

          case BuiltInFunctionDefinitions.JSON =>
            new JsonCallGen().generate(ctx, operands, FlinkTypeFactory.toLogicalType(call.getType))

          case _ =>
            new BridgingSqlFunctionCallGen(call).generate(ctx, operands, resultType)
        }

      // advanced scalar functions
      case sqlOperator: SqlOperator =>
        StringCallGen
          .generateCallExpression(ctx, call.getOperator, operands, resultType)
          .getOrElse {
            FunctionGenerator
              .getInstance(ctx.tableConfig)
              .getCallGenerator(sqlOperator, operands.map(expr => expr.resultType), resultType)
              .getOrElse(
                throw new CodeGenException(
                  s"Unsupported call: " +
                    s"$sqlOperator(${operands.map(_.resultType).mkString(", ")}) \n" +
                    s"If you think this function should be supported, " +
                    s"you can create an issue and start a discussion for it."))
              .generate(ctx, operands, resultType)
          }

      // unknown or invalid
      case call @ _ =>
        val explainCall = s"$call(${operands.map(_.resultType).mkString(", ")})"
        throw new CodeGenException(s"Unsupported call: $explainCall")
    }
  }

  private def generateJsonCall(operand: RexNode) = {
    val jsonCall = operand.asInstanceOf[RexCall]
    val jsonOperands = jsonCall.getOperands.map(_.accept(this))
    generateCallExpression(
      ctx,
      jsonCall,
      jsonOperands,
      FlinkTypeFactory.toLogicalType(jsonCall.getType))
  }

  def getOperandLiterals(operands: Seq[GeneratedExpression]): Array[AnyRef] = {
    operands.map {
      expr =>
        expr.literalValue match {
          case None => null
          case Some(literal) =>
            getConverterForDataType(fromLogicalTypeToDataType(expr.resultType))
              .asInstanceOf[DataFormatConverter[AnyRef, AnyRef]]
              .toExternal(literal.asInstanceOf[AnyRef])
        }
    }.toArray
  }
}
