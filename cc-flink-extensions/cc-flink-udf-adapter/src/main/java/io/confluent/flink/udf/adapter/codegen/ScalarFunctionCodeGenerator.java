/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.codegen;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ExprCodeGenerator;
import org.apache.flink.table.planner.codegen.FunctionCodeGenerator;
import org.apache.flink.table.planner.codegen.GeneratedExpression;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;
import org.apache.commons.text.StringSubstitutor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Does the actual code generation for UDFs. */
public class ScalarFunctionCodeGenerator {

    /**
     * Creates a {@link GeneratedFunction} which returns a {@link RichMapFunction}. This map can be
     * invoked with the parameters passed as inputs, and it returns the result directly.
     *
     * @param tableConfig The table config
     * @param functionName The function name to generate
     * @param returnLogicalType The return type
     * @param argumentLogicalTypes The argument types
     * @param classLoader The class loader
     * @param callNode The call node to generate code for
     * @return The resulting function, which can be instantiated and called
     */
    public static GeneratedFunction<RichMapFunction<RowData, Object>> generate(
            Configuration tableConfig,
            String functionName,
            LogicalType returnLogicalType,
            List<LogicalType> argumentLogicalTypes,
            ClassLoader classLoader,
            RexNode callNode) {
        RowType inputRowType = RowType.of(argumentLogicalTypes.toArray(new LogicalType[0]));
        RowType returnRowType = RowType.of(returnLogicalType);

        return generateFunction(
                inputRowType, functionName, returnRowType, callNode, tableConfig, classLoader);
    }

    /**
     * Generates code for the body of {@link RichMapFunction#map(Object)}. This logic invokes the
     * UDF eval call, converting arguments and results from/to internal types. The final result is
     * just returned.
     *
     * @param ctx the code generator context
     * @param inputType The row type of the inputs, which are the function arguments
     * @param callNode The RexNode of the function call
     * @param inputTerm The name of the input term of the map call
     * @return The java code to be compiled into the map call body
     */
    private static String generateProcessCode(
            CodeGeneratorContext ctx, RowType inputType, RexNode callNode, String inputTerm) {
        ExprCodeGenerator exprGenerator =
                new ExprCodeGenerator(ctx, false)
                        .bindInput(
                                inputType,
                                inputTerm,
                                JavaScalaConversionUtil.toScala(Optional.empty()));

        GeneratedExpression callExpr = exprGenerator.generateExpression(callNode);

        Map<String, String> values = new HashMap<>();
        values.put("resultTerm", callExpr.resultTerm());
        values.put("code", callExpr.code());

        return StringSubstitutor.replace(
                String.join(
                        "\n",
                        new String[] {
                            "  ${code}", "  return ${resultTerm};",
                        }),
                values);
    }

    /**
     * This takes the top level type info and RexNode, and generates the {@link RichMapFunction}
     * which will end up being called.
     *
     * @param inputType The row type of the inputs, which are the function arguments
     * @param name The name used for the generated class
     * @param returnType The row type of the result
     * @param callNode The RexNode of the function call
     * @param tableConfig The table config to use during generation
     * @param classLoader The class loader to use when loading classing during generation
     * @return The generated function
     */
    private static GeneratedFunction<RichMapFunction<RowData, Object>> generateFunction(
            RowType inputType,
            String name,
            RowType returnType,
            RexNode callNode,
            ReadableConfig tableConfig,
            ClassLoader classLoader) {
        CodeGeneratorContext ctx = new CodeGeneratorContext(tableConfig, classLoader);

        String processCode =
                generateProcessCode(ctx, inputType, callNode, CodeGenUtils.DEFAULT_INPUT1_TERM());
        return FunctionCodeGenerator.generateFunction(
                ctx,
                name,
                getFunctionClass(),
                processCode,
                returnType,
                inputType,
                CodeGenUtils.DEFAULT_INPUT1_TERM(),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                JavaScalaConversionUtil.toScala(Optional.empty()),
                CodeGenUtils.DEFAULT_COLLECTOR_TERM(),
                CodeGenUtils.DEFAULT_CONTEXT_TERM());
    }

    @SuppressWarnings("unchecked")
    private static Class<RichMapFunction<RowData, Object>> getFunctionClass() {
        return (Class<RichMapFunction<RowData, Object>>) (Object) MapFunction.class;
    }
}
