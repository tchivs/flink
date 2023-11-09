/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import java.util.List;

/**
 * Confluent-specific catalog that extends Flink's {@link Catalog}.
 *
 * <p>It provides additional methods that expose information about Confluent infra. For example,
 * compared to open source Flink both catalogs and databases can be referenced by unique ID or not
 * always unique display name.
 *
 * <p>All catalogs registered in a {@link TableEnvironment} should implement this interface.
 * Similarly, all tables should implement {@link ConfluentCatalogTable}.
 */
@Confluent
public interface ConfluentCatalog extends Catalog {

    /**
     * Extension of {@link #listDatabases()} for more information about Confluent-specific
     * databases.
     */
    List<DatabaseInfo> listDatabaseInfos() throws CatalogException;

    /** Returns information about this Confluent-specific catalog. */
    CatalogInfo getCatalogInfo();
}
