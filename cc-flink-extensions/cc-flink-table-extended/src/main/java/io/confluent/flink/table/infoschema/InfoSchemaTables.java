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
     * ID of the Information Schema database. Prefixed with $ to indicate a system database that
     * cannot collide with Confluent IDs.
     */
    public static final String INFORMATION_SCHEMA_DATABASE_ID = "$information-schema";

    /** Name of the Information Schema database. */
    public static final String INFORMATION_SCHEMA_DATABASE_NAME = "INFORMATION_SCHEMA";

    /** {@link DatabaseInfo} for Information Schema. */
    public static final DatabaseInfo INFORMATION_SCHEMA_DATABASE_INFO =
            DatabaseInfo.of(INFORMATION_SCHEMA_DATABASE_ID, INFORMATION_SCHEMA_DATABASE_NAME);

    /**
     * ID of the Definition Schema database. Prefixed with $ to indicate a system database that
     * cannot collide with Confluent IDs.
     */
    public static final String DEFINITION_SCHEMA_DATABASE_ID = "$metadata";

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
                        CatalogsStreamProvider.INSTANCE));

        tables.put(
                TABLE_CATALOG_NAME,
                InfoSchemaTable.of(
                        Schema.newBuilder()
                                .column("CATALOG_ID", "STRING NOT NULL")
                                .column("CATALOG_NAME", "STRING NOT NULL")
                                .build(),
                        CatalogNameStreamProvider.INSTANCE,
                        "CATALOG_ID"));

        tables.put(
                TABLE_SCHEMATA,
                InfoSchemaTable.of(
                        Schema.newBuilder()
                                .column("CATALOG_ID", "STRING NOT NULL")
                                .column("CATALOG_NAME", "STRING NOT NULL")
                                .column("SCHEMA_ID", "STRING NOT NULL")
                                .column("SCHEMA_NAME", "STRING NOT NULL")
                                .build(),
                        SchemataStreamProvider.INSTANCE,
                        "CATALOG_ID"));

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
            String tableName, SerdeContext context, Map<String, String> idColumns) {
        return TABLES.get(tableName).streamProvider.createStream(context, idColumns);
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
     * Helper for {@link InfoSchemaTableSource} that lists columns for identifying the requested
     * catalog.
     */
    public static Set<String> getCatalogIdColumns(String tableName) {
        final InfoSchemaTable table = TABLES.get(tableName);
        return table.catalogIdColumns;
    }

    // --------------------------------------------------------------------------------------------
    // Other helpers
    // --------------------------------------------------------------------------------------------

    /** Declaration of a schema table used for both the information and definition schema. */
    private static class InfoSchemaTable {
        final Schema schema;
        final Set<String> catalogIdColumns;
        final InfoTableStreamProvider streamProvider;

        InfoSchemaTable(
                Schema schema,
                Set<String> catalogIdColumns,
                InfoTableStreamProvider streamProvider) {
            this.schema = schema;
            this.catalogIdColumns = catalogIdColumns;
            this.streamProvider = streamProvider;
        }

        static InfoSchemaTable of(
                Schema schema, InfoTableStreamProvider streamProvider, String... catalogColumns) {
            return new InfoSchemaTable(
                    schema, new HashSet<>(Arrays.asList(catalogColumns)), streamProvider);
        }
    }

    /** Provider for data of the {@link InfoSchemaTable}. */
    interface InfoTableStreamProvider {
        Stream<GenericRowData> createStream(SerdeContext context, Map<String, String> idColumns);

        // ---
        // Common logic shared between InfoTableStreamProvider
        // ---

        static ConfluentCatalog getCatalog(SerdeContext context, String catalogId) {
            return context.getFlinkContext()
                    .getCatalogManager()
                    .getCatalog(catalogId)
                    .map(ConfluentCatalog.class::cast)
                    .orElseThrow(IllegalStateException::new);
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
        if (!table.catalogIdColumns.isEmpty()) {
            sql.append(" WHERE ");
            sql.append(
                    table.catalogIdColumns.stream()
                            .map(EncodingUtils::escapeIdentifier)
                            .map(
                                    c ->
                                            String.format(
                                                    "%s = '%s'",
                                                    c, EncodingUtils.escapeSingleQuotes(catalogId)))
                            .collect(Collectors.joining(" AND ")));
        }

        final String view = sql.toString();

        return CatalogView.of(table.schema, "SYSTEM", view, view, Collections.emptyMap());
    }

    private static ConfluentCatalogTable getTable(InfoSchemaTable table) {
        return new ConfluentCatalogTable(
                table.schema,
                "SYSTEM",
                Collections.emptyList(),
                Collections.singletonMap("connector", InfoSchemaTableFactory.IDENTIFIER),
                Collections.emptyMap());
    }
}
