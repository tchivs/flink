/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities around system columns and default watermarks.
 *
 * <p>System columns are a Confluent-specific concept that is powered by the catalog. System columns
 * and their defaults never end up in the persistence layer. Instead, they are dynamically added to
 * a table's schema on read access. The implementation guarantees that system columns and default
 * watermarks never interfere with user-provided intent.
 */
@Confluent
public final class SystemColumnUtil {

    private static class SystemColumn {
        final String name;
        final UnresolvedColumn unresolvedColumn;
        final Column resolvedColumn;

        private SystemColumn(
                String name, UnresolvedColumn unresolvedColumn, Column resolvedColumn) {
            this.name = name;
            this.unresolvedColumn = unresolvedColumn;
            this.resolvedColumn = resolvedColumn;
        }
    }

    private static final SystemColumn COLUMN_ROWTIME = createSystemColumnRowtime();

    private static final List<SystemColumn> SYSTEM_COLUMNS =
            Collections.singletonList(COLUMN_ROWTIME);

    private static final Set<Column> SYSTEM_COLUMNS_RESOLVED =
            SYSTEM_COLUMNS.stream().map(sys -> sys.resolvedColumn).collect(Collectors.toSet());

    private static final String DEFAULT_WATERMARK_RESOLVED = "`SOURCE_WATERMARK`()";

    /** Adjusts the schema for CREATE TABLE or ALTER TABLE. */
    public static ResolvedSchema forCreateOrAlterTable(ResolvedSchema schema) {
        // System columns might be set by the user either explicitly or implicitly (i.e. via CREATE
        // TABLE LIKE). The first step has to be the removal of those columns that might exist in
        // the middle of the schema (due to CREATE TABLE LIKE). During the check, the entire column
        // signature must match, otherwise we fail later if the reserved namespace is used
        // incorrectly.
        final List<Column> nonSystemColumns =
                schema.getColumns().stream()
                        .filter(c -> !isSystemColumn(c))
                        .collect(Collectors.toList());

        final List<WatermarkSpec> nonDefaultSpecs =
                schema.getWatermarkSpecs().stream()
                        .filter(
                                spec -> {
                                    final String timeColumn = spec.getRowtimeAttribute();
                                    final String serializedWatermark =
                                            spec.getWatermarkExpression().asSerializableString();
                                    if (serializedWatermark.equals(DEFAULT_WATERMARK_RESOLVED)) {
                                        if (!timeColumn.equals(COLUMN_ROWTIME.name)) {
                                            throw new ValidationException(
                                                    String.format(
                                                            "SOURCE_WATERMARK() can only be declared for the %s system column.",
                                                            COLUMN_ROWTIME.name));
                                        }
                                        return false;
                                    }
                                    return true;
                                })
                        .collect(Collectors.toList());

        return new ResolvedSchema(
                nonSystemColumns, nonDefaultSpecs, schema.getPrimaryKey().orElse(null));
    }

    /** Adjusts the schema for reading a table. */
    public static Schema forGetTable(Schema schema) {
        final Schema.Builder builder = Schema.newBuilder();
        builder.fromSchema(schema);

        // System columns are only added if they don't collide with existing columns.
        // This ensures that the physical schema has precedence. This case is only important for
        // inferred table. For manual table, system columns use a reserved namespace.
        boolean rowtimeAdded = false;
        for (SystemColumn sys : SYSTEM_COLUMNS) {
            if (schema.getColumns().stream()
                    .map(UnresolvedColumn::getName)
                    .noneMatch(n -> n.equals(sys.name))) {
                builder.fromColumns(Collections.singletonList(sys.unresolvedColumn));

                if (sys == COLUMN_ROWTIME) {
                    rowtimeAdded = true;
                }
            }
        }

        // Only add a default watermark strategy if there is no custom one
        // and the $rowtime system column is present.
        if (schema.getWatermarkSpecs().isEmpty() && rowtimeAdded) {
            builder.watermark(COLUMN_ROWTIME.name, DEFAULT_WATERMARK_RESOLVED);
        }

        return builder.build();
    }

    public static boolean isSystemColumn(Column column) {
        return SYSTEM_COLUMNS_RESOLVED.contains(column);
    }

    private static SystemColumn createSystemColumnRowtime() {
        final String name = "$rowtime";
        final DataType dataType = DataTypes.TIMESTAMP_LTZ(3).notNull();
        final String comment = "SYSTEM";

        return new SystemColumn(
                name,
                new UnresolvedMetadataColumn(name, dataType, null, true, comment),
                Column.metadata(name, dataType, null, true).withComment(comment));
    }

    private SystemColumnUtil() {}
}
