/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** {@link InfoTableStreamProvider} for {@link InfoSchemaTables#TABLE_KEY_COLUMN_USAGE}. */
@Confluent
class KeyColumnUsageStreamProvider extends InfoTableStreamProvider {

    static final KeyColumnUsageStreamProvider INSTANCE = new KeyColumnUsageStreamProvider();

    @Override
    public Stream<GenericRowData> createStream(
            SerdeContext context, Map<String, String> pushedFilters) {
        final String catalogId = pushedFilters.get("CONSTRAINT_CATALOG_ID");
        final String databaseId =
                pushedFilters.getOrDefault(
                        "CONSTRAINT_SCHEMA_ID", pushedFilters.get("TABLE_SCHEMA_ID"));
        final String databaseName =
                pushedFilters.getOrDefault("CONSTRAINT_SCHEMA", pushedFilters.get("TABLE_SCHEMA"));
        final String tableName = pushedFilters.get("TABLE_NAME");

        return getTableInfoStream(context, catalogId, databaseId, databaseName, tableName)
                .flatMap(
                        tableInfo ->
                                tableInfo
                                        .baseTable
                                        .getResolvedSchema()
                                        .getPrimaryKey()
                                        .map(c -> constraintToStream(tableInfo, c))
                                        .orElseGet(Stream::empty));
    }

    private static Stream<GenericRowData> constraintToStream(
            TableInfo tableInfo, UniqueConstraint c) {
        return IntStream.range(0, c.getColumns().size())
                .mapToObj(pos -> constraintToRow(tableInfo, c, pos));
    }

    private static GenericRowData constraintToRow(
            TableInfo tableInfo, UniqueConstraint constraint, int pos) {
        final GenericRowData out = new GenericRowData(12);

        // CONSTRAINT_CATALOG_ID, CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA_ID, CONSTRAINT_SCHEMA,
        // CONSTRAINT_NAME
        fillIdentifiers(
                out, 0, tableInfo.catalogInfo, tableInfo.databaseInfo, constraint.getName());

        // TABLE_CATALOG_ID, TABLE_CATALOG, TABLE_SCHEMA_ID, TABLE_SCHEMA, TABLE_NAME
        fillIdentifiers(out, 5, tableInfo.catalogInfo, tableInfo.databaseInfo, tableInfo.tableName);

        // COLUMN_NAME
        out.setField(10, StringData.fromString(constraint.getColumns().get(pos)));

        // ORDINAL_POSITION
        out.setField(11, pos + 1);

        return out;
    }

    private KeyColumnUsageStreamProvider() {}
}
