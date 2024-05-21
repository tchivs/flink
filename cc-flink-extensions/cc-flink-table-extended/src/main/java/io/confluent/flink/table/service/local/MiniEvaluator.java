/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.local;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;

import io.confluent.flink.table.service.summary.ExpressionKind;
import io.confluent.flink.table.service.summary.NodeKind;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A minimal evaluator that can translate a very limited set of {@link FlinkPhysicalRel} nodes and
 * run them locally.
 *
 * <p>Note that there are different ways of avoiding a full-blown Flink job. This hand-crafted
 * utility offers the lowest latency as it avoids code generation or any other additional overhead
 * (e.g. when running a Flink mini cluster for {@link StreamExecNode}s).
 */
@Confluent
public abstract class MiniEvaluator {

    public static final Set<NodeKind> SUPPORTED_NODES =
            EnumSet.of(NodeKind.SOURCE_SCAN, NodeKind.CALC, NodeKind.UNION);

    public static final Set<ExpressionKind> SUPPORTED_EXPRESSIONS =
            EnumSet.of(
                    ExpressionKind.EQUALS,
                    ExpressionKind.NOT_EQUALS,
                    ExpressionKind.SEARCH,
                    ExpressionKind.IS_NULL,
                    ExpressionKind.IS_NOT_NULL,
                    ExpressionKind.AND,
                    ExpressionKind.OR,
                    ExpressionKind.CAST,
                    ExpressionKind.INPUT_REF,
                    ExpressionKind.LITERAL);

    private final RexBuilder rexBuilder;

    protected MiniEvaluator(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    /**
     * Translates the given {@link FlinkPhysicalRel} into a consumable {@link Stream}.
     *
     * <p>Since this evaluator is very limited, it throws an {@link UnsupportedOperationException}
     * if the translation fails. This can happen for various reasons (e.g. unsupported nodes,
     * expressions, data types, configuration of nodes, etc.). The fallback should always be a
     * regular Flink job execution.
     */
    public Stream<GenericRowData> translate(RelNode relNode) throws UnsupportedOperationException {
        if (relNode instanceof StreamPhysicalUnion) {
            return translateUnion((StreamPhysicalUnion) relNode);
        } else if (relNode instanceof StreamPhysicalCalc) {
            return translateCalc((StreamPhysicalCalc) relNode);
        } else if (relNode instanceof StreamPhysicalTableSourceScan) {
            return translateScan((StreamPhysicalTableSourceScan) relNode);
        }
        throw new UnsupportedOperationException("Unsupported node: " + relNode);
    }

    protected abstract Stream<GenericRowData> translateScan(StreamPhysicalTableSourceScan scan);

    private Stream<GenericRowData> translateUnion(StreamPhysicalUnion union) {
        if (!union.all) {
            throw new UnsupportedOperationException("Unsupported node: " + union);
        }
        // This forces an eager translation before execution, otherwise translation errors
        // would be thrown in the hot path when processing data
        final List<Stream<GenericRowData>> inputs =
                union.getInputs().stream().map(this::translate).collect(Collectors.toList());
        return inputs.stream().flatMap(Function.identity());
    }

    private Stream<GenericRowData> translateCalc(StreamPhysicalCalc calc) {
        final RexProgram program = calc.getProgram();

        final ExpressionEvaluator condition;
        if (program.getCondition() != null) {
            condition = translateExpression(program.expandLocalRef(program.getCondition()));
        } else {
            condition = null;
        }

        final List<ExpressionEvaluator> projection =
                program.expandList(program.getProjectList()).stream()
                        .map(this::translateExpression)
                        .collect(Collectors.toList());

        final Stream<GenericRowData> input = translate(calc.getInput());

        final int size = projection.size();
        return input.filter(
                        in -> {
                            if (condition != null) {
                                final Boolean result = (Boolean) condition.eval(in);
                                return result != null && result;
                            }
                            return true;
                        })
                .map(
                        in -> {
                            final GenericRowData out = new GenericRowData(size);
                            for (int pos = 0; pos < size; pos++) {
                                out.setField(pos, projection.get(pos).eval(in));
                            }
                            return out;
                        });
    }

    // --------------------------------------------------------------------------------------------
    // Expressions
    // --------------------------------------------------------------------------------------------

    private ExpressionEvaluator translateExpression(RexNode withSearch) {
        final RexNode node = RexUtil.expandSearch(rexBuilder, null, withSearch);

        switch (node.getKind()) {
            case INPUT_REF:
                return translateInputRef(node);
            case LITERAL:
                return translateLiteral(node);
            case EQUALS:
                return translateEquals(node, Object::equals);
            case NOT_EQUALS:
                return translateEquals(node, (l, r) -> !l.equals(r));
            case AND:
                return translateThreeValuedLogic(
                        node,
                        (l, r) -> {
                            // False && Unknown -> False
                            if (Boolean.FALSE.equals(l) && r == null) {
                                return false;
                            }
                            // Unknown && False -> False
                            if (l == null && Boolean.FALSE.equals(r)) {
                                return false;
                            }
                            return null;
                        },
                        (l, r) -> l && r);
            case OR:
                return translateThreeValuedLogic(
                        node,
                        (l, r) -> {
                            // True || Unknown -> True
                            if (Boolean.TRUE.equals(l) && r == null) {
                                return true;
                            }
                            // Unknown || True -> True
                            if (l == null && Boolean.TRUE.equals(r)) {
                                return true;
                            }
                            return null;
                        },
                        (l, r) -> l || r);
            case CAST:
                return translateCast(node);
            case IS_NULL:
                return translateIsNull(node);
            case IS_NOT_NULL:
                return translateIsNotNull(node);
        }
        throw new UnsupportedOperationException("Unsupported expression: " + node);
    }

    private ExpressionEvaluator translateInputRef(RexNode node) {
        final RexInputRef inputRef = (RexInputRef) node;
        final int srcPos = inputRef.getIndex();
        return in -> in.getField(srcPos);
    }

    private ExpressionEvaluator translateLiteral(RexNode node) {
        final RexLiteral literal = (RexLiteral) node;
        final SqlTypeName type = literal.getType().getSqlTypeName();
        if (literal.isNull()) {
            return in -> null;
        } else if (type == SqlTypeName.CHAR || type == SqlTypeName.VARCHAR) {
            final String value = literal.getValueAs(String.class);
            return in -> StringData.fromString(value);
        } else if (type == SqlTypeName.INTEGER) {
            final Integer value = literal.getValueAs(Integer.class);
            return in -> value;
        }
        throw new UnsupportedOperationException("Unsupported literal: " + node);
    }

    private ExpressionEvaluator translateEquals(
            RexNode node, BiFunction<Object, Object, Boolean> op) {
        final RexCall call = (RexCall) node;
        final ExpressionEvaluator left = translateExpression(call.getOperands().get(0));
        final ExpressionEvaluator right = translateExpression(call.getOperands().get(1));
        return in -> {
            final Object leftResult = left.eval(in);
            final Object rightResult = right.eval(in);
            if (leftResult == null || rightResult == null) {
                return null;
            }
            return op.apply(leftResult, rightResult);
        };
    }

    private ExpressionEvaluator translateIsNull(RexNode node) {
        final RexCall call = (RexCall) node;
        final ExpressionEvaluator arg = translateExpression(call.getOperands().get(0));
        return (in) -> arg.eval(in) == null;
    }

    private ExpressionEvaluator translateIsNotNull(RexNode node) {
        final RexCall call = (RexCall) node;
        final ExpressionEvaluator arg = translateExpression(call.getOperands().get(0));
        return (in) -> arg.eval(in) != null;
    }

    private ExpressionEvaluator translateThreeValuedLogic(
            RexNode node,
            BiFunction<Boolean, Boolean, Boolean> withUnknown,
            BiFunction<Boolean, Boolean, Boolean> withoutUnknown) {
        final RexCall call = (RexCall) node;
        final ExpressionEvaluator left = translateExpression(call.getOperands().get(0));
        final ExpressionEvaluator right = translateExpression(call.getOperands().get(1));
        return in -> {
            final Boolean leftResult = (Boolean) left.eval(in);
            final Boolean rightResult = (Boolean) right.eval(in);
            // Three-valued logic:
            // no Unknown -> two-valued logic
            if (leftResult != null && rightResult != null) {
                return withoutUnknown.apply(leftResult, rightResult);
            }
            return withUnknown.apply(leftResult, rightResult);
        };
    }

    private ExpressionEvaluator translateCast(RexNode node) {
        final RexCall call = (RexCall) node;
        final RelDataType targetType = call.getType();

        if (!isSupportedCastType(targetType)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported cast to '%s'. Currently, only STRING and INT types are supported.",
                            node.getType()));
        }

        final ExpressionEvaluator input = translateExpression(call.getOperands().get(0));
        if (targetType.getSqlTypeName() == SqlTypeName.VARCHAR) {
            return in -> {
                Object result = input.eval(in);
                return result == null ? null : StringData.fromString(result.toString());
            };
        } else if (targetType.getSqlTypeName() == SqlTypeName.INTEGER) {
            return in -> {
                Object result = input.eval(in);
                return result == null ? null : Integer.valueOf(result.toString());
            };
        }
        throw new UnsupportedOperationException("Unsupported cast: " + node);
    }

    private boolean isSupportedCastType(RelDataType dataType) {
        return (dataType.getSqlTypeName() == SqlTypeName.VARCHAR
                        && dataType.getPrecision() == Integer.MAX_VALUE)
                || dataType.getSqlTypeName() == SqlTypeName.INTEGER;
    }

    private interface ExpressionEvaluator {
        Object eval(GenericRowData in);
    }
}
