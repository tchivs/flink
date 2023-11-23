/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.ConfluentCatalog;
import io.confluent.flink.table.infoschema.InfoSchemaTables.InfoTableStreamProvider;

import java.util.Map;
import java.util.stream.Stream;

/** {@link InfoTableStreamProvider} for {@link InfoSchemaTables#TABLE_SCHEMATA}. */
@Confluent
class SchemataStreamProvider implements InfoTableStreamProvider {

    static final SchemataStreamProvider INSTANCE = new SchemataStreamProvider();

    @Override
    public Stream<GenericRowData> createStream(
            SerdeContext context, Map<String, String> idColumns) {
        final String catalogId = idColumns.get("CATALOG_ID");
        final ConfluentCatalog catalog = InfoTableStreamProvider.getCatalog(context, catalogId);
        final CatalogInfo catalogInfo = catalog.getCatalogInfo();

        return catalog.listDatabaseInfos().stream()
                .map(
                        databaseInfo -> {
                            final GenericRowData out = new GenericRowData(4);
                            out.setField(0, StringData.fromString(catalogInfo.getId()));
                            out.setField(1, StringData.fromString(catalogInfo.getName()));
                            out.setField(2, StringData.fromString(databaseInfo.getId()));
                            out.setField(3, StringData.fromString(databaseInfo.getName()));
                            return out;
                        });
    }

    private SchemataStreamProvider() {}
}
