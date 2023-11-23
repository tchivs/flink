/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.ConfluentCatalog;
import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.catalog.ConfluentSystemCatalog;
import io.confluent.flink.table.catalog.DatabaseInfo;
import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundLocalResultPlan;
import io.confluent.flink.table.service.ResultPlanUtils;
import io.confluent.flink.table.service.ServiceTasks;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end tests for {@link InfoSchemaTables} using {@link ServiceTasks}. */
@Confluent
public class InfoSchemaExecutionTest {

    private static TableEnvironment tableEnv;

    @BeforeAll
    static void setUp() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final List<CatalogInfo> catalogInfos =
                Arrays.asList(CatalogInfo.of("env-1", "cat1"), CatalogInfo.of("env-2", "cat2"));

        tableEnv.registerCatalog(
                ConfluentSystemCatalog.ID, new ConfluentSystemCatalog(catalogInfos));
        catalogInfos.forEach(
                catalogInfo -> {
                    final ConfluentCatalog catalog =
                            new CatalogWithInfoSchema(
                                    catalogInfo,
                                    Arrays.asList(
                                            DatabaseInfo.of("lkc-1", "db1"),
                                            DatabaseInfo.of("lkc-2", "db2")));
                    tableEnv.registerCatalog(catalogInfo.getId(), catalog);
                    tableEnv.registerCatalog(catalogInfo.getName(), catalog);
                });
        tableEnv.useCatalog("cat2");
        tableEnv.useDatabase("db1");

        tableEnv.executeSql("CREATE TABLE sink (s STRING) WITH ('connector' = 'blackhole')");
        tableEnv.executeSql("CREATE TABLE simple_source (s STRING) WITH ('connector' = 'datagen')");
    }

    @Test
    void testListCatalogs() throws Exception {
        assertResult(
                "SELECT * FROM `INFORMATION_SCHEMA`.`CATALOGS`",
                row("env-1", "cat1"),
                row("env-2", "cat2"));
    }

    @Test
    void testListCatalogName() throws Exception {
        assertResult(
                "SELECT * FROM `INFORMATION_SCHEMA`.`INFORMATION_SCHEMA_CATALOG_NAME`",
                row("env-2", "cat2"));
    }

    @Test
    void testListSchemata() throws Exception {
        assertResult(
                "SELECT * FROM `INFORMATION_SCHEMA`.`SCHEMATA`",
                row("env-2", "cat2", "lkc-1", "db1"),
                row("env-2", "cat2", "lkc-2", "db2"),
                row("env-2", "cat2", "$information-schema", "INFORMATION_SCHEMA"));
    }

    @Test
    void testListSchemataWithFilter() throws Exception {
        assertResult(
                "SELECT * FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE `SCHEMA_NAME` <> 'INFORMATION_SCHEMA'",
                row("env-2", "cat2", "lkc-1", "db1"),
                row("env-2", "cat2", "lkc-2", "db2"));
    }

    @Test
    void testUnsupportedBackgroundQuery() {
        assertThatThrownBy(
                        () ->
                                ResultPlanUtils.backgroundJob(
                                        tableEnv,
                                        "INSERT INTO sink SELECT `SCHEMA_NAME` "
                                                + "FROM `INFORMATION_SCHEMA`.`SCHEMATA`"))
                .hasMessageContaining(
                        "Access to INFORMATION_SCHEMA is currently limited. "
                                + "Only foreground statements are supported.");
    }

    @Test
    void testUnsupportedMixWithRegularTables() {
        assertUnsupported(
                "(SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`) "
                        + "UNION ALL (SELECT s FROM simple_source)",
                "Access to INFORMATION_SCHEMA is currently limited. "
                        + "INFORMATION_SCHEMA queries must not access regular tables.");
    }

    @Test
    void testUnsupportedOperations() {
        assertUnsupported(
                "SELECT COUNT(*) FROM `INFORMATION_SCHEMA`.`SCHEMATA`",
                "Access to INFORMATION_SCHEMA is currently limited. "
                        + "Only basic SQL queries (such as SELECT, WHERE, and UNION ALL) are supported.");

        assertUnsupported(
                "SELECT UPPER(`SCHEMA_NAME`) FROM `INFORMATION_SCHEMA`.`SCHEMATA`",
                "Access to INFORMATION_SCHEMA is currently limited. "
                        + "Only basic SQL expressions (such as <>, =, AND, OR, IS NULL) are supported.");

        assertUnsupported(
                "SELECT CAST(`SCHEMA_NAME` AS BYTES) FROM `INFORMATION_SCHEMA`.`SCHEMATA`",
                "Access to INFORMATION_SCHEMA is currently limited. "
                        + "Unsupported cast to 'VARBINARY(2147483647)'. Currently, only STRING and INT types are supported.");
    }

    @Test
    void testAccessToDefinitionSchema() {
        assertUnsupported(
                "SELECT `SCHEMA_NAME` FROM `$system`.`$metadata`.`SCHEMATA`",
                "Table '`$system`.`$metadata`.`SCHEMATA`' cannot "
                        + "be accessed without providing the required columns: [CATALOG_ID]");
    }

    private static void assertResult(String selectSql, GenericRowData... expectedData)
            throws Exception {
        final ForegroundLocalResultPlan localResult =
                ResultPlanUtils.foregroundLocal(tableEnv, selectSql);
        assertThat(localResult.getData()).containsExactlyInAnyOrder(expectedData);
    }

    private static void assertUnsupported(String selectSql, String errorMessage) {
        assertThatThrownBy(() -> ResultPlanUtils.foregroundLocal(tableEnv, selectSql))
                .hasMessageContaining(errorMessage);
    }

    /** Helper method to construct {@link GenericRowData} while keeping the tests readable. */
    private static GenericRowData row(Object... objects) {
        return GenericRowData.of(
                Stream.of(objects)
                        .map(
                                o -> {
                                    if (o instanceof String) {
                                        return StringData.fromString((String) o);
                                    }
                                    return o;
                                })
                        .toArray());
    }

    private static class CatalogWithInfoSchema extends GenericInMemoryCatalog
            implements ConfluentCatalog {

        final CatalogInfo catalogInfo;
        final List<DatabaseInfo> databaseInfos;

        public CatalogWithInfoSchema(CatalogInfo catalogInfo, List<DatabaseInfo> databaseInfos) {
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
            // List INFORMATION_SCHEMA views
            // For testing purposes only by name.
            return Stream.concat(
                            InfoSchemaTables.listViewsByName(databaseName).stream(),
                            super.listViews(databaseName).stream())
                    .collect(Collectors.toList());
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
            // Lookup INFORMATION_SCHEMA tables.
            // For testing purposes only by name.
            final Optional<CatalogView> infoSchemaTable =
                    InfoSchemaTables.getViewByName(catalogInfo, tablePath);
            if (infoSchemaTable.isPresent()) {
                return infoSchemaTable.get();
            }
            final CatalogBaseTable baseTable = super.getTable(tablePath);
            if (baseTable.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
                return baseTable;
            }
            // Make sure we always return ConfluentCatalogTable
            final CatalogTable table = (CatalogTable) baseTable;
            return new ConfluentCatalogTable(
                    table.getUnresolvedSchema(),
                    table.getComment(),
                    table.getPartitionKeys(),
                    table.getOptions(),
                    Collections.emptyMap());
        }

        @Override
        public List<DatabaseInfo> listDatabaseInfos() {
            // Add INFORMATION_SCHEMA to databases
            return Stream.concat(
                            databaseInfos.stream(),
                            Stream.of(InfoSchemaTables.INFORMATION_SCHEMA_DATABASE_INFO))
                    .collect(Collectors.toList());
        }

        @Override
        public CatalogInfo getCatalogInfo() {
            return catalogInfo;
        }
    }
}
