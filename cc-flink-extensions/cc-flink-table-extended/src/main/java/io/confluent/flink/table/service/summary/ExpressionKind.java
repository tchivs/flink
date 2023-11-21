/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Kind of SQL expression. */
@Confluent
public enum ExpressionKind {
    // For all other expressions that currently,
    // don't need a specific classification.
    OTHER(null),

    // Add more items to the list if there is a need for it.
    // The list could also be more specific in the future,
    // not just SQL kind but also Confluent-provided functions.
    EQUALS(SqlKind.EQUALS),
    NOT_EQUALS(SqlKind.NOT_EQUALS),
    SEARCH(SqlKind.SEARCH),
    IS_NULL(SqlKind.IS_NULL),
    IS_NOT_NULL(SqlKind.IS_NOT_NULL),
    AND(SqlKind.AND),
    OR(SqlKind.OR),
    CAST(SqlKind.CAST),
    INPUT_REF(SqlKind.INPUT_REF),
    LITERAL(SqlKind.LITERAL);

    private final @Nullable SqlKind sqlKind;

    private static final Map<SqlKind, ExpressionKind> TO_KIND =
            Stream.of(ExpressionKind.values())
                    .collect(Collectors.toMap(kind -> kind.sqlKind, kind -> kind));

    ExpressionKind(@Nullable SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public @Nullable SqlKind getSqlKind() {
        return sqlKind;
    }

    static ExpressionKind mapToKind(SqlKind sqlKind) {
        return TO_KIND.getOrDefault(sqlKind, OTHER);
    }

    // --------------------------------------------------------------------------------------------
    // Logic for ExpressionKind extraction
    // --------------------------------------------------------------------------------------------

    static class Extractor extends RexVisitorImpl<Void> {

        final Set<ExpressionKind> kinds = EnumSet.noneOf(ExpressionKind.class);

        Extractor() {
            super(true);
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            addKind(inputRef);
            return super.visitInputRef(inputRef);
        }

        @Override
        public Void visitLocalRef(RexLocalRef localRef) {
            // will always be expanded
            return super.visitLocalRef(localRef);
        }

        @Override
        public Void visitLiteral(RexLiteral literal) {
            addKind(literal);
            return super.visitLiteral(literal);
        }

        @Override
        public Void visitOver(RexOver over) {
            addKind(over);
            return super.visitOver(over);
        }

        @Override
        public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
            addKind(correlVariable);
            return super.visitCorrelVariable(correlVariable);
        }

        @Override
        public Void visitCall(RexCall call) {
            addKind(call);
            return super.visitCall(call);
        }

        @Override
        public Void visitDynamicParam(RexDynamicParam dynamicParam) {
            addKind(dynamicParam);
            return super.visitDynamicParam(dynamicParam);
        }

        @Override
        public Void visitRangeRef(RexRangeRef rangeRef) {
            addKind(rangeRef);
            return super.visitRangeRef(rangeRef);
        }

        @Override
        public Void visitFieldAccess(RexFieldAccess fieldAccess) {
            addKind(fieldAccess);
            return super.visitFieldAccess(fieldAccess);
        }

        @Override
        public Void visitSubQuery(RexSubQuery subQuery) {
            addKind(subQuery);
            return super.visitSubQuery(subQuery);
        }

        @Override
        public Void visitTableInputRef(RexTableInputRef ref) {
            addKind(ref);
            return super.visitTableInputRef(ref);
        }

        @Override
        public Void visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            addKind(fieldRef);
            return super.visitPatternFieldRef(fieldRef);
        }

        private void addKind(RexNode node) {
            kinds.add(ExpressionKind.mapToKind(node.getKind()));
        }
    }
}
