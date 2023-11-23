/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;

import io.confluent.flink.table.connectors.ConfluentManagedTableValidator;

import java.util.Map;
import java.util.stream.Stream;

/** {@link InfoTableStreamProvider} for {@link InfoSchemaTables#TABLE_TABLE_OPTIONS}. */
@Confluent
class TableOptionsStreamProvider extends InfoTableStreamProvider {

    static final TableOptionsStreamProvider INSTANCE = new TableOptionsStreamProvider();

    @Override
    public Stream<GenericRowData> createStream(
            SerdeContext context, Map<String, String> pushedFilters) {
        final String catalogId = pushedFilters.get("TABLE_CATALOG_ID");
        final String databaseId = pushedFilters.get("TABLE_SCHEMA_ID");
        final String databaseName = pushedFilters.get("TABLE_SCHEMA");
        final String tableName = pushedFilters.get("TABLE_NAME");

        return getTableInfoStream(context, catalogId, databaseId, databaseName, tableName)
                .flatMap(
                        tableInfo -> {
                            final ResolvedCatalogBaseTable<?> baseTable =
                                    tableInfo.getBaseTable(context);

                            final Map<String, String> filteredOptions =
                                    ConfluentManagedTableValidator.filterForPublicOptions(
                                            baseTable.getOptions());
                            return filteredOptions.entrySet().stream()
                                    .map(e -> optionToRow(tableInfo, e.getKey(), e.getValue()));
                        });
    }

    private static GenericRowData optionToRow(TableInfo tableInfo, String key, String value) {
        final GenericRowData out = new GenericRowData(7);

        // TABLE_CATALOG_ID, TABLE_CATALOG, TABLE_SCHEMA_ID, TABLE_SCHEMA, TABLE_NAME
        fillIdentifiers(out, 0, tableInfo.catalogInfo, tableInfo.databaseInfo, tableInfo.tableName);

        // OPTION_KEY
        out.setField(5, StringData.fromString(key));
        // OPTION_VALUE
        out.setField(6, StringData.fromString(value));

        return out;
    }

    private TableOptionsStreamProvider() {}
}
