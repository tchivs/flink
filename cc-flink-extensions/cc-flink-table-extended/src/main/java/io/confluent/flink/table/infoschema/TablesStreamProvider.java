/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;
import org.apache.flink.util.StringUtils;

import io.confluent.flink.table.catalog.SystemColumnUtil;

import java.util.Map;
import java.util.stream.Stream;

/** {@link InfoTableStreamProvider} for {@link InfoSchemaTables#TABLE_TABLES}. */
@Confluent
class TablesStreamProvider extends InfoTableStreamProvider {

    private static final String TABLE_TYPE_BASE_TABLE = "BASE TABLE";
    private static final String TABLE_TYPE_VIEW = "VIEW";

    private static final String DISTRIBUTION_ALGORITHM_HASH = "HASH";

    static final TablesStreamProvider INSTANCE = new TablesStreamProvider();

    @Override
    public Stream<GenericRowData> createStream(
            SerdeContext context, Map<String, String> pushedFilters) {
        final String catalogId = pushedFilters.get("TABLE_CATALOG_ID");
        final String databaseId = pushedFilters.get("TABLE_SCHEMA_ID");
        final String databaseName = pushedFilters.get("TABLE_SCHEMA");
        final String tableName = pushedFilters.get("TABLE_NAME");

        return getTableInfoStream(context, catalogId, databaseId, databaseName, tableName)
                .map(TablesStreamProvider::tableInfoToRow);
    }

    private static GenericRowData tableInfoToRow(TableInfo tableInfo) {
        final ResolvedCatalogBaseTable<?> baseTable = tableInfo.baseTable;

        final GenericRowData out = new GenericRowData(14);

        // CATALOG_ID, CATALOG_NAME, SCHEMA_ID, SCHEMA_NAME, TABLE_NAME
        fillIdentifiers(out, 0, tableInfo.catalogInfo, tableInfo.databaseInfo, tableInfo.tableName);

        final String tableType;
        String isDistributed = NO;
        String distributionAlgorithm = null;
        String distributionBuckets = null;
        String isWatermarked = NO;
        String watermarkColumn = null;
        String watermarkExpression = null;
        String watermarkIsHidden = null;
        if (baseTable.getTableKind() == TableKind.TABLE) {
            tableType = TABLE_TYPE_BASE_TABLE;

            final ResolvedCatalogTable table = (ResolvedCatalogTable) baseTable;

            // TODO rework this after DISTRIBUTED BY is supported
            if (table.getPartitionKeys().size() > 0) {
                isDistributed = YES;
                distributionAlgorithm = DISTRIBUTION_ALGORITHM_HASH;
            }

            final ResolvedSchema schema = baseTable.getResolvedSchema();
            if (schema.getWatermarkSpecs().size() > 0) {
                // Reiterate if we support more than one watermark spec, but
                // this is unlikely.
                final WatermarkSpec spec = schema.getWatermarkSpecs().get(0);
                isWatermarked = YES;
                watermarkColumn = spec.getRowtimeAttribute();
                watermarkExpression = spec.getWatermarkExpression().asSerializableString();
                final boolean hasSystemColumn =
                        SystemColumnUtil.isSystemColumn(
                                schema.getColumn(watermarkColumn).orElse(null));
                watermarkIsHidden = hasSystemColumn ? YES : NO;
            }
        } else {
            tableType = TABLE_TYPE_VIEW;
        }
        // TABLE_TYPE
        out.setField(5, StringData.fromString(tableType));
        // IS_DISTRIBUTED
        out.setField(6, StringData.fromString(isDistributed));
        // DISTRIBUTION_ALGORITHM
        out.setField(7, StringData.fromString(distributionAlgorithm));
        // DISTRIBUTION_BUCKETS
        out.setField(8, StringData.fromString(distributionBuckets));
        // IS_WATERMARKED
        out.setField(9, StringData.fromString(isWatermarked));
        // WATERMARK_COLUMN
        out.setField(10, StringData.fromString(watermarkColumn));
        // WATERMARK_EXPRESSION
        out.setField(11, StringData.fromString(watermarkExpression));
        // WATERMARK_IS_HIDDEN
        out.setField(12, StringData.fromString(watermarkIsHidden));

        String comment = baseTable.getComment();
        if (StringUtils.isNullOrWhitespaceOnly(comment)) {
            comment = null;
        }
        // COMMENT
        out.setField(13, StringData.fromString(comment));

        return out;
    }

    private TablesStreamProvider() {}
}
