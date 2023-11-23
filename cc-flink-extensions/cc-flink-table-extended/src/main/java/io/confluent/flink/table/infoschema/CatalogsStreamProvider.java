/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;

import io.confluent.flink.table.catalog.ConfluentSystemCatalog;
import io.confluent.flink.table.infoschema.InfoSchemaTables.InfoTableStreamProvider;

import java.util.Map;
import java.util.stream.Stream;

/** {@link InfoTableStreamProvider} for {@link InfoSchemaTables#TABLE_CATALOGS}. */
@Confluent
class CatalogsStreamProvider implements InfoTableStreamProvider {

    static final CatalogsStreamProvider INSTANCE = new CatalogsStreamProvider();

    @Override
    public Stream<GenericRowData> createStream(
            SerdeContext context, Map<String, String> idColumns) {
        final ConfluentSystemCatalog accountCatalog =
                (ConfluentSystemCatalog)
                        InfoTableStreamProvider.getCatalog(context, ConfluentSystemCatalog.ID);

        return accountCatalog.getCatalogInfos().stream()
                .map(
                        e -> {
                            final GenericRowData out = new GenericRowData(2);
                            out.setField(0, StringData.fromString(e.getId()));
                            out.setField(1, StringData.fromString(e.getName()));
                            return out;
                        });
    }

    private CatalogsStreamProvider() {}
}
