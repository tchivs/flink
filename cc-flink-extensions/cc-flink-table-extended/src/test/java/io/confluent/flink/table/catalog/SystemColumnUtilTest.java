/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SystemColumnUtil}. */
@Confluent
public class SystemColumnUtilTest {

    @Test
    void testCreateOrAlterTable() {
        final ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("p1", DataTypes.INT()),
                                Column.metadata("m1", DataTypes.STRING(), null, true),
                                Column.physical("p2", DataTypes.INT()),
                                Column.metadata(
                                                "$rowtime",
                                                DataTypes.TIMESTAMP_LTZ(3).notNull(),
                                                null,
                                                true)
                                        .withComment("SYSTEM"),
                                Column.physical("p3", DataTypes.INT())),
                        Collections.singletonList(
                                WatermarkSpec.of("$rowtime", new WatermarkExpression())),
                        null);

        final ResolvedSchema schemaWithoutSystemColumns =
                SystemColumnUtil.forCreateOrAlterTable(schema);

        assertThat(schemaWithoutSystemColumns.toString())
                .isEqualTo(
                        "(\n"
                                + "  `p1` INT,\n"
                                + "  `m1` STRING METADATA VIRTUAL,\n"
                                + "  `p2` INT,\n"
                                + "  `p3` INT\n"
                                + ")");
    }

    @Test
    void testCreateOrAlterTableWithInvalidSourceWatermark() {
        final ResolvedSchema schema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.metadata(
                                        "ts", DataTypes.TIMESTAMP_LTZ(3).notNull(), null, true),
                                Column.physical("p", DataTypes.INT())),
                        Collections.singletonList(
                                WatermarkSpec.of("ts", new WatermarkExpression())),
                        null);

        assertThatThrownBy(() -> SystemColumnUtil.forCreateOrAlterTable(schema))
                .hasMessageContaining(
                        "SOURCE_WATERMARK() can only be declared for the $rowtime system column.");
    }

    @Test
    void testGetTable() {
        final Schema schema = Schema.newBuilder().column("p", DataTypes.INT()).build();

        final Schema schemaWithSystemColumns = SystemColumnUtil.forGetTable(schema);

        assertThat(schemaWithSystemColumns.toString())
                .isEqualTo(
                        "(\n"
                                + "  `p` INT,\n"
                                + "  `$rowtime` METADATA VIRTUAL COMMENT 'SYSTEM',\n"
                                + "  WATERMARK FOR `$rowtime` AS [`SOURCE_WATERMARK`()]\n"
                                + ")");
    }

    @Test
    void testGetTableWithConflictingName() {
        final Schema schema =
                Schema.newBuilder()
                        .column("p", DataTypes.INT())
                        .column("$rowtime", DataTypes.TIMESTAMP(3))
                        .watermark("$rowtime", "$rowtime - INTERVAL '2' SECOND")
                        .build();

        final Schema schemaWithSystemColumns = SystemColumnUtil.forGetTable(schema);

        // The $rowtime column might be derived from Schema Registry and has a different
        // type. It is not touched. Also, the watermark remains untouched.
        assertThat(schemaWithSystemColumns.toString())
                .isEqualTo(
                        "(\n"
                                + "  `p` INT,\n"
                                + "  `$rowtime` TIMESTAMP(3),\n"
                                + "  WATERMARK FOR `$rowtime` AS [$rowtime - INTERVAL '2' SECOND]\n"
                                + ")");
    }

    @Test
    void testGetTableWithCustomWatermark() {
        final Schema schema =
                Schema.newBuilder()
                        .column("p", DataTypes.INT())
                        .watermark("$rowtime", "$rowtime - INTERVAL '2' SECOND")
                        .build();

        final Schema schemaWithSystemColumns = SystemColumnUtil.forGetTable(schema);

        // The $rowtime column is added. The watermark remains untouched.
        assertThat(schemaWithSystemColumns.toString())
                .isEqualTo(
                        "(\n"
                                + "  `p` INT,\n"
                                + "  `$rowtime` METADATA VIRTUAL COMMENT 'SYSTEM',\n"
                                + "  WATERMARK FOR `$rowtime` AS [$rowtime - INTERVAL '2' SECOND]\n"
                                + ")");
    }

    private static class WatermarkExpression implements ResolvedExpression {

        @Override
        public String asSerializableString() {
            return "`SOURCE_WATERMARK`()";
        }

        @Override
        public DataType getOutputDataType() {
            return DataTypes.TIMESTAMP_LTZ(3).notNull();
        }

        @Override
        public List<ResolvedExpression> getResolvedChildren() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String asSummaryString() {
            return asSerializableString();
        }

        @Override
        public List<Expression> getChildren() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> expressionVisitor) {
            throw new UnsupportedOperationException();
        }
    }
}
