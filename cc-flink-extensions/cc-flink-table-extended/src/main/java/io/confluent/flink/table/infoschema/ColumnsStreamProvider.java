/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import io.confluent.flink.table.catalog.SystemColumnUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** {@link InfoTableStreamProvider} for {@link InfoSchemaTables#TABLE_COLUMNS}. */
@Confluent
class ColumnsStreamProvider extends InfoTableStreamProvider {

    static final ColumnsStreamProvider INSTANCE = new ColumnsStreamProvider();

    private static final Map<LogicalTypeRoot, String> ROOT_AS_STRING = initRootAsString();

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
                            final ResolvedCatalogBaseTable<?> baseTable = tableInfo.baseTable;

                            final Map<String, Integer> distributionKeys;
                            if (baseTable.getTableKind() == TableKind.TABLE) {
                                final ResolvedCatalogTable resolvedTable =
                                        (ResolvedCatalogTable) baseTable;
                                final List<String> bucketKeys =
                                        resolvedTable
                                                .getDistribution()
                                                .map(TableDistribution::getBucketKeys)
                                                .orElseGet(Collections::emptyList);
                                distributionKeys =
                                        IntStream.range(0, bucketKeys.size())
                                                .boxed()
                                                .collect(
                                                        Collectors.toMap(
                                                                bucketKeys::get, pos -> pos + 1));
                            } else {
                                distributionKeys = Collections.emptyMap();
                            }
                            final List<Column> columns = baseTable.getResolvedSchema().getColumns();

                            return IntStream.range(0, columns.size())
                                    .mapToObj(
                                            i ->
                                                    columnToRow(
                                                            tableInfo,
                                                            columns.get(i),
                                                            i,
                                                            distributionKeys));
                        });
    }

    private static GenericRowData columnToRow(
            TableInfo tableInfo, Column column, int pos, Map<String, Integer> distributionKeys) {
        final GenericRowData out = new GenericRowData(18);

        // TABLE_CATALOG_ID, TABLE_CATALOG, TABLE_SCHEMA_ID, TABLE_SCHEMA, TABLE_NAME
        fillIdentifiers(out, 0, tableInfo.catalogInfo, tableInfo.databaseInfo, tableInfo.tableName);

        final LogicalType type = column.getDataType().getLogicalType();
        // COLUMN_NAME
        out.setField(5, StringData.fromString(column.getName()));
        // ORDINAL_POSITION
        out.setField(6, pos + 1);
        // IS_NULLABLE
        out.setField(7, StringData.fromString(type.isNullable() ? YES : NO));

        final String rootAsString = ROOT_AS_STRING.get(type.getTypeRoot());
        if (rootAsString == null) {
            throw new UnsupportedOperationException(
                    "Unexpected data type root: " + type.getTypeRoot());
        }
        // DATA_TYPE
        out.setField(8, StringData.fromString(rootAsString));

        // FULL_DATA_TYPE
        out.setField(9, StringData.fromString(type.copy(true).asSerializableString()));

        final boolean isSystemColumn = SystemColumnUtil.isSystemColumn(column);
        // IS_HIDDEN
        out.setField(10, StringData.fromString(isSystemColumn ? YES : NO));

        final boolean isComputed = column instanceof ComputedColumn;
        final String generatedExpression;
        if (isComputed) {
            final ComputedColumn computedColumn = (ComputedColumn) column;
            generatedExpression = computedColumn.getExpression().asSerializableString();
        } else {
            generatedExpression = null;
        }
        // IS_GENERATED
        out.setField(11, StringData.fromString(isComputed ? YES : NO));
        // GENERATION_EXPRESSION
        out.setField(12, StringData.fromString(generatedExpression));

        final boolean isMetadata = column instanceof MetadataColumn;
        final String metadataKey;
        if (isMetadata) {
            final MetadataColumn metadataColumn = (MetadataColumn) column;
            metadataKey = metadataColumn.getMetadataKey().orElse(null);
        } else {
            metadataKey = null;
        }
        // IS_METADATA
        out.setField(13, StringData.fromString(isMetadata ? YES : NO));
        // METADATA_KEY
        out.setField(14, StringData.fromString(metadataKey));

        // IS_PERSISTED
        out.setField(15, StringData.fromString(column.isPersisted() ? YES : NO));

        // DISTRIBUTION_ORDINAL_POSITION
        out.setField(16, distributionKeys.get(column.getName()));

        // COMMENT
        out.setField(17, StringData.fromString(column.getComment().orElse(null)));

        return out;
    }

    private static Map<LogicalTypeRoot, String> initRootAsString() {
        final Map<LogicalTypeRoot, String> map = new HashMap<>();
        map.put(LogicalTypeRoot.CHAR, "CHAR");
        map.put(LogicalTypeRoot.VARCHAR, "VARCHAR");
        map.put(LogicalTypeRoot.BOOLEAN, "BOOLEAN");
        map.put(LogicalTypeRoot.BINARY, "BINARY");
        map.put(LogicalTypeRoot.VARBINARY, "VARBINARY");
        map.put(LogicalTypeRoot.DECIMAL, "DECIMAL");
        map.put(LogicalTypeRoot.TINYINT, "TINYINT");
        map.put(LogicalTypeRoot.SMALLINT, "SMALLINT");
        map.put(LogicalTypeRoot.INTEGER, "INTEGER");
        map.put(LogicalTypeRoot.BIGINT, "BIGINT");
        map.put(LogicalTypeRoot.FLOAT, "FLOAT");
        map.put(LogicalTypeRoot.DOUBLE, "DOUBLE");
        map.put(LogicalTypeRoot.DATE, "DATE");
        map.put(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE, "TIME");
        map.put(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, "TIMESTAMP");
        map.put(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, "TIMESTAMP_LTZ");
        map.put(LogicalTypeRoot.INTERVAL_YEAR_MONTH, "INTERVAL");
        map.put(LogicalTypeRoot.INTERVAL_DAY_TIME, "INTERVAL");
        map.put(LogicalTypeRoot.ARRAY, "ARRAY");
        map.put(LogicalTypeRoot.MULTISET, "MULTISET");
        map.put(LogicalTypeRoot.MAP, "MAP");
        map.put(LogicalTypeRoot.ROW, "ROW");
        map.put(LogicalTypeRoot.DISTINCT_TYPE, "USER-DEFINED");
        map.put(LogicalTypeRoot.STRUCTURED_TYPE, "USER-DEFINED");
        map.put(LogicalTypeRoot.NULL, "NULL");
        return map;
    }

    private ColumnsStreamProvider() {}
}
