/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.ConfluentCatalog;
import io.confluent.flink.table.catalog.DatabaseInfo;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** Provider of data for {@link InfoSchemaTables}. */
@Confluent
public abstract class InfoTableStreamProvider {

    protected static final String YES = "YES";
    protected static final String NO = "NO";

    abstract Stream<GenericRowData> createStream(
            SerdeContext context, Map<String, String> pushedFilters);

    // ---
    // Common logic shared between InfoTableStreamProvider subclasses
    // ---

    protected static ConfluentCatalog getCatalog(SerdeContext context, String catalogId) {
        return context.getFlinkContext()
                .getCatalogManager()
                .getCatalog(catalogId)
                .map(ConfluentCatalog.class::cast)
                .orElseThrow(IllegalStateException::new);
    }

    protected static Stream<TableInfo> getTableInfoStream(
            SerdeContext context,
            String catalogId,
            @Nullable String databaseId,
            @Nullable String databaseName,
            @Nullable String tableName) {
        final ConfluentCatalog catalog = getCatalog(context, catalogId);
        final CatalogInfo catalogInfo = catalog.getCatalogInfo();

        final Predicate<DatabaseInfo> databaseFilter =
                info ->
                        (databaseId == null || databaseId.equals(info.getId()))
                                && (databaseName == null || databaseName.equals(info.getName()));

        final Predicate<TableInfo> tableFilter =
                info -> tableName == null || tableName.equals(info.tableName);

        return catalog.listDatabaseInfos().stream()
                .filter(databaseFilter)
                .flatMap(
                        databaseInfo -> {
                            final List<String> tables;
                            try {
                                tables = catalog.listTables(databaseInfo.getId());
                            } catch (DatabaseNotExistException e) {
                                return Stream.empty();
                            }

                            final List<String> views;
                            try {
                                views = catalog.listViews(databaseInfo.getId());
                            } catch (DatabaseNotExistException e) {
                                return Stream.empty();
                            }

                            return Stream.concat(tables.stream(), views.stream())
                                    .map(t -> new TableInfo(catalogInfo, databaseInfo, t))
                                    .filter(tableFilter);
                        });
    }

    /** Helper object to represent a table with catalog and database origins. */
    protected static class TableInfo {
        protected final CatalogInfo catalogInfo;
        protected final DatabaseInfo databaseInfo;
        protected final String tableName;

        TableInfo(CatalogInfo catalogInfo, DatabaseInfo databaseInfo, String tableName) {
            this.catalogInfo = catalogInfo;
            this.databaseInfo = databaseInfo;
            this.tableName = tableName;
        }

        ResolvedCatalogBaseTable<?> getResolvedTable(SerdeContext context) {
            final ObjectIdentifier id =
                    ObjectIdentifier.of(catalogInfo.getId(), databaseInfo.getId(), tableName);
            return context.getFlinkContext()
                    .getCatalogManager()
                    .getTableOrError(id)
                    .getResolvedTable();
        }

        CatalogBaseTable getUnresolvedTable(SerdeContext context) {
            try {
                return context.getFlinkContext()
                        .getCatalogManager()
                        .getCatalog(catalogInfo.getId())
                        .orElseThrow(IllegalStateException::new)
                        .getTable(new ObjectPath(databaseInfo.getId(), tableName));
            } catch (TableNotExistException | IllegalStateException e) {
                // Should never happen, just for a consistent exception
                return getResolvedTable(context);
            }
        }
    }

    protected static void fillIdentifiers(
            GenericRowData row,
            int startPos,
            CatalogInfo catalogInfo,
            DatabaseInfo databaseInfo,
            @Nullable String tableName) {
        row.setField(startPos, StringData.fromString(catalogInfo.getId()));
        row.setField(startPos + 1, StringData.fromString(catalogInfo.getName()));
        row.setField(startPos + 2, StringData.fromString(databaseInfo.getId()));
        row.setField(startPos + 3, StringData.fromString(databaseInfo.getName()));
        if (tableName != null) {
            row.setField(startPos + 4, StringData.fromString(tableName));
        }
    }
}
