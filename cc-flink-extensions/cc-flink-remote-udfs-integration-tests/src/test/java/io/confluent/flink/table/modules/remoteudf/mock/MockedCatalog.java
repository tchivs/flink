/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.mock;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.catalog.ConfluentCatalogView;
import io.confluent.flink.table.catalog.DatabaseInfo;
import io.confluent.flink.table.infoschema.InfoSchemaTables;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A test catalog for registering the UDFs. */
public class MockedCatalog extends GenericInMemoryCatalog implements Catalog {

    private final CatalogInfo catalogInfo;
    private final List<DatabaseInfo> databaseInfos;

    public MockedCatalog(CatalogInfo catalogInfo, List<DatabaseInfo> databaseInfos) {
        super(catalogInfo.getName(), "ignored");
        this.catalogInfo = catalogInfo;
        this.databaseInfos = databaseInfos;
    }

    @Override
    public String getDefaultDatabase() {
        return null;
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() {
        return listDatabaseInfos().stream()
                .flatMap(i -> Stream.of(i.getId(), i.getName()))
                .collect(Collectors.toList());
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException {
        // Include INFORMATION_SCHEMA views
        return Stream.of(
                        InfoSchemaTables.listViewsByName(databaseName).stream(),
                        InfoSchemaTables.listViewsById(databaseName).stream(),
                        super.listViews(databaseName).stream())
                .flatMap(Function.identity())
                .collect(Collectors.toList());
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        // Include INFORMATION_SCHEMA tables
        final Optional<CatalogView> infoSchemaById =
                InfoSchemaTables.getViewById(catalogInfo, tablePath);
        if (infoSchemaById.isPresent()) {
            return infoSchemaById.get();
        }
        final Optional<CatalogView> infoSchemaByName =
                InfoSchemaTables.getViewByName(catalogInfo, tablePath);
        if (infoSchemaByName.isPresent()) {
            return infoSchemaByName.get();
        }

        final CatalogBaseTable baseTable = super.getTable(tablePath);
        if (baseTable.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
            // Make sure we always return ConfluentCatalogView
            final CatalogView view = (CatalogView) baseTable;
            return new ConfluentCatalogView(
                    view.getUnresolvedSchema(),
                    view.getComment(),
                    view.getOriginalQuery(),
                    view.getExpandedQuery());
        }
        // Make sure we always return ConfluentCatalogTable
        final CatalogTable table = (CatalogTable) baseTable;
        return new ConfluentCatalogTable(
                table.getUnresolvedSchema(),
                table.getComment(),
                null,
                table.getPartitionKeys(),
                table.getOptions());
    }

    public List<DatabaseInfo> listDatabaseInfos() {
        // Add INFORMATION_SCHEMA to databases
        return Stream.concat(
                        databaseInfos.stream(),
                        Stream.of(InfoSchemaTables.INFORMATION_SCHEMA_DATABASE_INFO))
                .collect(Collectors.toList());
    }

    public CatalogInfo getCatalogInfo() {
        return catalogInfo;
    }
}
