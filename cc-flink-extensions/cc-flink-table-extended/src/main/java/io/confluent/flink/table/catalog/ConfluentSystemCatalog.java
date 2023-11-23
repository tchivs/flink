/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

import io.confluent.flink.table.infoschema.InfoSchemaTables;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A {@link ConfluentCatalog} that provides a system-defined, read-only shared catalog that contains
 * metadata about the objects in the user's organization.
 */
@Confluent
public class ConfluentSystemCatalog implements ConfluentCatalog {

    /**
     * ID of this catalog. Prefixed with $ to indicate a system catalog that cannot collide with
     * Confluent IDs.
     */
    public static final String ID = "$system";

    /**
     * For now this catalog has no own name. Once we officially expose the catalog, we can choose an
     * actual name while the catalog can still be referenced by ID.
     */
    public static final CatalogInfo INFO = CatalogInfo.of(ID, ID);

    private final List<CatalogInfo> catalogInfos;

    public ConfluentSystemCatalog(List<CatalogInfo> catalogInfo) {
        this.catalogInfos = catalogInfo;
    }

    public List<CatalogInfo> getCatalogInfos() {
        return catalogInfos;
    }

    @Override
    public List<DatabaseInfo> listDatabaseInfos() throws CatalogException {
        return Collections.singletonList(InfoSchemaTables.DEFINITION_SCHEMA_DATABASE_INFO);
    }

    @Override
    public CatalogInfo getCatalogInfo() {
        return INFO;
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public @Nullable String getDefaultDatabase() {
        return null;
    }

    @Override
    public List<String> listDatabases() {
        return Collections.singletonList(InfoSchemaTables.DEFINITION_SCHEMA_DATABASE_ID);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) {
        throw new CatalogException("Method 'getDatabase' is not implemented.");
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return databaseName.equals(InfoSchemaTables.DEFINITION_SCHEMA_DATABASE_ID);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public List<String> listTables(String databaseName) {
        return InfoSchemaTables.listBaseTablesById(databaseName);
    }

    @Override
    public List<String> listViews(String databaseName) {
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
        final Optional<ConfluentCatalogTable> table = InfoSchemaTables.getBaseTableById(tablePath);
        if (table.isPresent()) {
            return table.get();
        }
        throw new TableNotExistException(INFO.getId(), tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) {
        final Optional<ConfluentCatalogTable> table = InfoSchemaTables.getBaseTableById(tablePath);
        return table.isPresent();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        throw new CatalogException("Method 'getPartition' is not implemented.");
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public List<String> listFunctions(String dbName) {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws CatalogException {
        throw new CatalogException("Method 'getFunction' is not implemented.");
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath,
            CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) {
        throw new ValidationException("The catalog is immutable.");
    }
}
