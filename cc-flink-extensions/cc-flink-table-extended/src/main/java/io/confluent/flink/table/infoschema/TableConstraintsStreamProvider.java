/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.Constraint.ConstraintType;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;

import java.util.Map;
import java.util.stream.Stream;

/** {@link InfoTableStreamProvider} for {@link InfoSchemaTables#TABLE_TABLE_CONSTRAINTS}. */
@Confluent
class TableConstraintsStreamProvider extends InfoTableStreamProvider {

    static final TableConstraintsStreamProvider INSTANCE = new TableConstraintsStreamProvider();

    private static final String CONSTRAINT_TYPE_PRIMARY_KEY = "PRIMARY KEY";
    private static final String CONSTRAINT_TYPE_UNIQUE = "UNIQUE";

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
                                        .map(c -> Stream.of(constraintToRow(tableInfo, c)))
                                        .orElseGet(Stream::empty));
    }

    private static GenericRowData constraintToRow(
            TableInfo tableInfo, UniqueConstraint constraint) {
        final GenericRowData out = new GenericRowData(12);

        // CONSTRAINT_CATALOG_ID, CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA_ID, CONSTRAINT_SCHEMA,
        // CONSTRAINT_NAME
        fillIdentifiers(
                out, 0, tableInfo.catalogInfo, tableInfo.databaseInfo, constraint.getName());

        // TABLE_CATALOG_ID, TABLE_CATALOG, TABLE_SCHEMA_ID, TABLE_SCHEMA, TABLE_NAME
        fillIdentifiers(out, 5, tableInfo.catalogInfo, tableInfo.databaseInfo, tableInfo.tableName);

        // CONSTRAINT_TYPE
        final ConstraintType constraintType = constraint.getType();
        final String constraintString;
        switch (constraintType) {
            case PRIMARY_KEY:
                constraintString = CONSTRAINT_TYPE_PRIMARY_KEY;
                break;
            case UNIQUE_KEY:
                constraintString = CONSTRAINT_TYPE_UNIQUE;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown constraint type: " + constraintType);
        }
        out.setField(10, StringData.fromString(constraintString));

        // ENFORCED
        out.setField(11, StringData.fromString(constraint.isEnforced() ? YES : NO));

        return out;
    }

    private TableConstraintsStreamProvider() {}
}
