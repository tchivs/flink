/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;
import org.apache.flink.table.utils.EncodingUtils;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.ConfluentCatalog;
import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.catalog.ConfluentSystemCatalog;
import io.confluent.flink.table.catalog.DatabaseInfo;
import io.confluent.flink.table.connectors.InfoSchemaTableFactory;
import io.confluent.flink.table.connectors.InfoSchemaTableSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Registry of generated (virtual) table declarations to power the INFORMATION_SCHEMA.
 *
 * <p>Part 11 (SQL/Schemata) of the SQL Standard defines two schemas: an Information Schema and a
 * Definition Schema. The Information Schema contains views (virtual tables) that are targeted to
 * the catalog they belong. The Definition Schema contains tables (base tables) that globally
 * reference all catalogs - at least conceptually. The purpose of Definition Schema is to provide a
 * data model for supporting Information Schema. Both Information Schema and Definition Schema have
 * identical column names and types.
 *
 * <p>Note: The SQL standard uses the term "schema" whereas Flink uses the term "database". In
 * Flink, this was chosen to avoid confusion with the schema part of a table (i.e. {@link Schema}).
 * However, to ensure compatibility with the standard, information schema uses the term "schema"
 * instead of "database".
 *
 * <p>A {@link ConfluentCatalog} can expose the Information Schema by calling {@link
 * #listViewsById(String)} and {@link #listViewsByName(String)} internally. The views will reference
 * {@link ConfluentSystemCatalog} as the single source of truth. This system catalog contains the
 * Definition Schema and provides base tables via {@link #getBaseTableById(ObjectPath)}. The base
 * tables use {@link InfoSchemaTableFactory} as connector.
 */
@Confluent
public class InfoSchemaTables {

    /**
     * ID of the Information Schema database. Prefixed with "db-" to indicate a system database that
     * cannot collide with Confluent IDs.
     */
    public static final String INFORMATION_SCHEMA_DATABASE_ID = "db-information-schema";

    /** Name of the Information Schema database. */
    public static final String INFORMATION_SCHEMA_DATABASE_NAME = "INFORMATION_SCHEMA";

    /** {@link DatabaseInfo} for Information Schema. */
    public static final DatabaseInfo INFORMATION_SCHEMA_DATABASE_INFO =
            DatabaseInfo.of(INFORMATION_SCHEMA_DATABASE_ID, INFORMATION_SCHEMA_DATABASE_NAME);

    /**
     * ID of the Definition Schema database. Prefixed with "db-" to indicate a system database that
     * cannot collide with Confluent IDs.
     */
    public static final String DEFINITION_SCHEMA_DATABASE_ID = "db-metadata";

    /** {@link DatabaseInfo} for Definition Schema. */
    public static final DatabaseInfo DEFINITION_SCHEMA_DATABASE_INFO =
            DatabaseInfo.of(DEFINITION_SCHEMA_DATABASE_ID, DEFINITION_SCHEMA_DATABASE_ID);

    // --------------------------------------------------------------------------------------------
    // Tables
    // --------------------------------------------------------------------------------------------

    /**
     * A table that contains a row for every catalog the user has access to.
     *
     * <p>This is an extension to the SQL standard and a special case as it allows to access
     * information across all catalogs.
     */
    public static final String TABLE_CATALOGS = "CATALOGS";

    /** A table that contains a single row for the catalog the information schema belongs to. */
    public static final String TABLE_CATALOG_NAME = "INFORMATION_SCHEMA_CATALOG_NAME";

    /** A table that contains a row for every database the user has access to. */
    public static final String TABLE_SCHEMATA = "SCHEMATA";

    /**
     * Synonym of {@link #TABLE_SCHEMATA} using Flink terminology to avoid confusion with the
     * standard compliant table name.
     */
    public static final String TABLE_DATABASES = "DATABASES";

    /** A table that contains a row for every (view/base) table the user has access to. */
    public static final String TABLE_TABLES = "TABLES";

    /**
     * A table that contains a row for every option (i.e. property in the WITH clause) of all tables
     * the user has access to.
     */
    public static final String TABLE_TABLE_OPTIONS = "TABLE_OPTIONS";

    /** A table that contains a row for every column of all tables the user has access to. */
    public static final String TABLE_COLUMNS = "COLUMNS";

    /**
     * A table that contains a row for every constraint that is a defined for a table the user has
     * access to.
     */
    public static final String TABLE_TABLE_CONSTRAINTS = "TABLE_CONSTRAINTS";

    /**
     * A table that contains a row for every column that is a defined in a table constraint the user
     * has access to.
     */
    public static final String TABLE_KEY_COLUMN_USAGE = "KEY_COLUMN_USAGE";

    private static final Map<String, InfoSchemaTable> TABLES = initTables();

    private static Map<String, InfoSchemaTable> initTables() {
        final Map<String, InfoSchemaTable> tables = new HashMap<>();

        tables.put(
                TABLE_CATALOGS,
                InfoSchemaTable.of(
                                Schema.newBuilder()
                                        .column("CATALOG_ID", "STRING NOT NULL")
                                        .column("CATALOG_NAME", "STRING NOT NULL")
                                        .build(),
                                CatalogsStreamProvider.INSTANCE)
                        .build());

        tables.put(
                TABLE_CATALOG_NAME,
                InfoSchemaTable.of(
                                Schema.newBuilder()
                                        .column("CATALOG_ID", "STRING NOT NULL")
                                        .column("CATALOG_NAME", "STRING NOT NULL")
                                        .build(),
                                CatalogNameStreamProvider.INSTANCE)
                        .withCatalogIdColumn("CATALOG_ID")
                        .build());

        tables.put(
                TABLE_SCHEMATA,
                InfoSchemaTable.of(
                                Schema.newBuilder()
                                        .column("CATALOG_ID", "STRING NOT NULL")
                                        .column("CATALOG_NAME", "STRING NOT NULL")
                                        .column("SCHEMA_ID", "STRING NOT NULL")
                                        .column("SCHEMA_NAME", "STRING NOT NULL")
                                        .build(),
                                SchemataStreamProvider.INSTANCE)
                        .withCatalogIdColumn("CATALOG_ID")
                        .build());

        tables.put(TABLE_DATABASES, tables.get(TABLE_SCHEMATA));

        tables.put(
                TABLE_TABLES,
                InfoSchemaTable.of(
                                Schema.newBuilder()
                                        .column("TABLE_CATALOG_ID", "STRING NOT NULL")
                                        .column("TABLE_CATALOG", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA_ID", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA", "STRING NOT NULL")
                                        .column("TABLE_NAME", "STRING NOT NULL")
                                        .column("TABLE_TYPE", "STRING NOT NULL")
                                        .column("IS_DISTRIBUTED", "STRING NOT NULL")
                                        .column("DISTRIBUTION_ALGORITHM", "STRING NULL")
                                        .column("DISTRIBUTION_BUCKETS", "INT NULL")
                                        .column("IS_WATERMARKED", "STRING NOT NULL")
                                        .column("WATERMARK_COLUMN", "STRING NULL")
                                        .column("WATERMARK_EXPRESSION", "STRING NULL")
                                        .column("WATERMARK_IS_HIDDEN", "STRING NULL")
                                        .column("COMMENT", "STRING NULL")
                                        .build(),
                                TablesStreamProvider.INSTANCE)
                        .withCatalogIdColumn("TABLE_CATALOG_ID")
                        .withFilterColumns("TABLE_SCHEMA_ID", "TABLE_SCHEMA", "TABLE_NAME")
                        .build());

        tables.put(
                TABLE_TABLE_OPTIONS,
                InfoSchemaTable.of(
                                Schema.newBuilder()
                                        .column("TABLE_CATALOG_ID", "STRING NOT NULL")
                                        .column("TABLE_CATALOG", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA_ID", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA", "STRING NOT NULL")
                                        .column("TABLE_NAME", "STRING NOT NULL")
                                        .column("OPTION_KEY", "STRING NOT NULL")
                                        .column("OPTION_VALUE", "STRING NOT NULL")
                                        .build(),
                                TableOptionsStreamProvider.INSTANCE)
                        .withCatalogIdColumn("TABLE_CATALOG_ID")
                        .withFilterColumns("TABLE_SCHEMA_ID", "TABLE_SCHEMA", "TABLE_NAME")
                        .build());

        tables.put(
                TABLE_COLUMNS,
                InfoSchemaTable.of(
                                Schema.newBuilder()
                                        .column("TABLE_CATALOG_ID", "STRING NOT NULL")
                                        .column("TABLE_CATALOG", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA_ID", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA", "STRING NOT NULL")
                                        .column("TABLE_NAME", "STRING NOT NULL")
                                        .column("COLUMN_NAME", "STRING NOT NULL")
                                        .column("ORDINAL_POSITION", "INT NOT NULL")
                                        .column("IS_NULLABLE", "STRING NOT NULL")
                                        .column("DATA_TYPE", "STRING NOT NULL")
                                        .column("FULL_DATA_TYPE", "STRING NOT NULL")
                                        .column("IS_HIDDEN", "STRING NOT NULL")
                                        .column("IS_GENERATED", "STRING NOT NULL")
                                        .column("GENERATION_EXPRESSION", "STRING NULL")
                                        .column("IS_METADATA", "STRING NOT NULL")
                                        .column("METADATA_KEY", "STRING NULL")
                                        .column("IS_PERSISTED", "STRING NOT NULL")
                                        .column("DISTRIBUTION_ORDINAL_POSITION", "INT NULL")
                                        .column("COMMENT", "STRING NULL")
                                        .build(),
                                ColumnsStreamProvider.INSTANCE)
                        .withCatalogIdColumn("TABLE_CATALOG_ID")
                        .withFilterColumns("TABLE_SCHEMA_ID", "TABLE_SCHEMA", "TABLE_NAME")
                        .build());

        tables.put(
                TABLE_TABLE_CONSTRAINTS,
                InfoSchemaTable.of(
                                Schema.newBuilder()
                                        .column("CONSTRAINT_CATALOG_ID", "STRING NOT NULL")
                                        .column("CONSTRAINT_CATALOG", "STRING NOT NULL")
                                        .column("CONSTRAINT_SCHEMA_ID", "STRING NOT NULL")
                                        .column("CONSTRAINT_SCHEMA", "STRING NOT NULL")
                                        .column("CONSTRAINT_NAME", "STRING NOT NULL")
                                        .column("TABLE_CATALOG_ID", "STRING NOT NULL")
                                        .column("TABLE_CATALOG", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA_ID", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA", "STRING NOT NULL")
                                        .column("TABLE_NAME", "STRING NOT NULL")
                                        .column("CONSTRAINT_TYPE", "STRING NOT NULL")
                                        .column("ENFORCED", "STRING NOT NULL")
                                        .build(),
                                TableConstraintsStreamProvider.INSTANCE)
                        .withCatalogIdColumn("CONSTRAINT_CATALOG_ID")
                        // TABLE_CATALOG_ID is always equal to CONSTRAINT_CATALOG_ID. Once we lift
                        // the requirement on having a catalog ID column we should include
                        // TABLE_CATALOG_ID in the filter push down columns.
                        .withFilterColumns(
                                "CONSTRAINT_SCHEMA_ID",
                                "CONSTRAINT_SCHEMA",
                                "TABLE_SCHEMA_ID",
                                "TABLE_SCHEMA",
                                "TABLE_NAME")
                        .build());

        tables.put(
                TABLE_KEY_COLUMN_USAGE,
                InfoSchemaTable.of(
                                Schema.newBuilder()
                                        .column("CONSTRAINT_CATALOG_ID", "STRING NOT NULL")
                                        .column("CONSTRAINT_CATALOG", "STRING NOT NULL")
                                        .column("CONSTRAINT_SCHEMA_ID", "STRING NOT NULL")
                                        .column("CONSTRAINT_SCHEMA", "STRING NOT NULL")
                                        .column("CONSTRAINT_NAME", "STRING NOT NULL")
                                        .column("TABLE_CATALOG_ID", "STRING NOT NULL")
                                        .column("TABLE_CATALOG", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA_ID", "STRING NOT NULL")
                                        .column("TABLE_SCHEMA", "STRING NOT NULL")
                                        .column("TABLE_NAME", "STRING NOT NULL")
                                        .column("COLUMN_NAME", "STRING NOT NULL")
                                        .column("ORDINAL_POSITION", "INT NOT NULL")
                                        .build(),
                                KeyColumnUsageStreamProvider.INSTANCE)
                        .withCatalogIdColumn("CONSTRAINT_CATALOG_ID")
                        .withFilterColumns(
                                "CONSTRAINT_SCHEMA_ID",
                                "CONSTRAINT_SCHEMA",
                                "TABLE_SCHEMA_ID",
                                "TABLE_SCHEMA",
                                "TABLE_NAME")
                        .build());

        return tables;
    }

    // --------------------------------------------------------------------------------------------
    // ConfluentCatalog
    // --------------------------------------------------------------------------------------------

    /** Helper to list Information Schema views for {@link ConfluentCatalog}. */
    public static List<String> listViewsById(String databaseId) {
        if (!databaseId.equals(INFORMATION_SCHEMA_DATABASE_ID)) {
            return Collections.emptyList();
        }
        return new ArrayList<>(TABLES.keySet());
    }

    /** Helper to list Information Schema views for {@link ConfluentCatalog}. */
    public static List<String> listViewsByName(String databaseName) {
        if (!databaseName.equals(INFORMATION_SCHEMA_DATABASE_NAME)) {
            return Collections.emptyList();
        }
        return new ArrayList<>(TABLES.keySet());
    }

    /**
     * Helper to return Information Schema views for {@link ConfluentCatalog} using the database ID.
     */
    public static Optional<CatalogView> getViewById(CatalogInfo catalogInfo, ObjectPath path) {
        if (!path.getDatabaseName().equals(INFORMATION_SCHEMA_DATABASE_ID)) {
            return Optional.empty();
        }
        final String tableName = path.getObjectName();
        return Optional.ofNullable(TABLES.get(tableName))
                .map(t -> InfoSchemaTables.getView(catalogInfo.getId(), tableName, t));
    }

    /**
     * Helper to return Information Schema views for {@link ConfluentCatalog} using the database
     * name.
     */
    public static Optional<CatalogView> getViewByName(CatalogInfo catalogInfo, ObjectPath path) {
        if (!path.getDatabaseName().equals(INFORMATION_SCHEMA_DATABASE_NAME)) {
            return Optional.empty();
        }
        final String tableName = path.getObjectName();
        return Optional.ofNullable(TABLES.get(tableName))
                .map(t -> InfoSchemaTables.getView(catalogInfo.getId(), tableName, t));
    }

    /** Helper to produce a stream of data for the requested Information Schema table. */
    public static Stream<GenericRowData> getStreamForTable(
            String tableName, SerdeContext context, Map<String, String> pushedFilters) {
        return TABLES.get(tableName).streamProvider.createStream(context, pushedFilters);
    }

    // --------------------------------------------------------------------------------------------
    // ConfluentAccountCatalog
    // --------------------------------------------------------------------------------------------

    /** Helper to list Definition Schema base tables for {@link ConfluentSystemCatalog}. */
    public static List<String> listBaseTablesById(String databaseId) {
        if (!databaseId.equals(DEFINITION_SCHEMA_DATABASE_ID)) {
            return Collections.emptyList();
        }
        return new ArrayList<>(TABLES.keySet());
    }

    /** Helper to return Definition Schema base tables for {@link ConfluentSystemCatalog}. */
    public static Optional<ConfluentCatalogTable> getBaseTableById(ObjectPath path) {
        if (!path.getDatabaseName().equals(DEFINITION_SCHEMA_DATABASE_ID)) {
            return Optional.empty();
        }
        return Optional.ofNullable(TABLES.get(path.getObjectName()))
                .map(InfoSchemaTables::getTable);
    }

    // --------------------------------------------------------------------------------------------
    // InfoSchemaTableSource
    // --------------------------------------------------------------------------------------------

    /**
     * Helper for {@link InfoSchemaTableSource} that returns the column for identifying the
     * requested catalog.
     */
    public static Optional<String> getCatalogIdColumn(String tableName) {
        final InfoSchemaTable table = TABLES.get(tableName);
        return Optional.ofNullable(table.catalogIdColumn);
    }

    /** Helper for {@link InfoSchemaTableSource} that lists columns for filter push down. */
    public static Set<String> getFilterColumns(String tableName) {
        final InfoSchemaTable table = TABLES.get(tableName);
        return Stream.concat(Stream.of(table.catalogIdColumn), table.filterColumns.stream())
                .collect(Collectors.toSet());
    }

    // --------------------------------------------------------------------------------------------
    // Other helpers
    // --------------------------------------------------------------------------------------------

    /** Declaration of a schema table used for both the information and definition schema. */
    private static class InfoSchemaTable {
        final Schema schema;
        final @Nullable String catalogIdColumn;
        final Set<String> filterColumns;
        final InfoTableStreamProvider streamProvider;

        InfoSchemaTable(
                Schema schema,
                @Nullable String catalogIdColumn,
                Set<String> filterColumns,
                InfoTableStreamProvider streamProvider) {
            this.schema = schema;
            this.catalogIdColumn = catalogIdColumn;
            this.filterColumns = filterColumns;
            this.streamProvider = streamProvider;
        }

        static Builder of(Schema schema, InfoTableStreamProvider streamProvider) {
            return new Builder(schema, streamProvider);
        }

        static class Builder {

            final Schema schema;
            final InfoTableStreamProvider streamProvider;

            @Nullable String catalogIdColumn;
            Set<String> filterColumns = new HashSet<>();

            Builder(Schema schema, InfoTableStreamProvider streamProvider) {
                this.schema = schema;
                this.streamProvider = streamProvider;
            }

            Builder withCatalogIdColumn(String catalogIdColumn) {
                this.catalogIdColumn = catalogIdColumn;
                return this;
            }

            Builder withFilterColumns(String... filterColumns) {
                this.filterColumns.addAll(Arrays.asList(filterColumns));
                return this;
            }

            InfoSchemaTable build() {
                return new InfoSchemaTable(schema, catalogIdColumn, filterColumns, streamProvider);
            }
        }
    }

    private static CatalogView getView(String catalogId, String tableName, InfoSchemaTable table) {
        final StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append(
                table.schema.getColumns().stream()
                        .map(UnresolvedColumn::getName)
                        .map(EncodingUtils::escapeIdentifier)
                        .collect(Collectors.joining(", ")));
        sql.append(" FROM ");
        sql.append(EncodingUtils.escapeIdentifier(ConfluentSystemCatalog.ID));
        sql.append(".");
        sql.append(EncodingUtils.escapeIdentifier(DEFINITION_SCHEMA_DATABASE_ID));
        sql.append(".");
        sql.append(EncodingUtils.escapeIdentifier(tableName));
        if (table.catalogIdColumn != null) {
            sql.append(" WHERE ");
            sql.append(EncodingUtils.escapeIdentifier(table.catalogIdColumn));
            sql.append(" = ");
            sql.append("'");
            sql.append(EncodingUtils.escapeSingleQuotes(catalogId));
            sql.append("'");
        }

        final String view = sql.toString();

        return CatalogView.of(table.schema, "SYSTEM", view, view, Collections.emptyMap());
    }

    private static ConfluentCatalogTable getTable(InfoSchemaTable table) {
        return new ConfluentCatalogTable(
                table.schema,
                "SYSTEM",
                null,
                Collections.emptyList(),
                Collections.singletonMap("connector", InfoSchemaTableFactory.IDENTIFIER),
                Collections.emptyMap());
    }
}
