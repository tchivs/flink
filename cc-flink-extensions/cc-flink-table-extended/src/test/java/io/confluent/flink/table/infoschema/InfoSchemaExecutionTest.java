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
import java.util.function.Function;
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
        tableEnv.useCatalog("env-2");
        tableEnv.useDatabase("lkc-1");

        tableEnv.executeSql(
                "CREATE TABLE `env-2`.`lkc-1`.`simple_sink` (s STRING) WITH ('connector' = 'blackhole')");
        tableEnv.executeSql(
                "CREATE TABLE `env-2`.`lkc-1`.`simple_source` (s STRING) WITH ('connector' = 'datagen')");

        tableEnv.executeSql(
                "CREATE TABLE `env-1`.`lkc-1`.`table1` (\n"
                        + "s STRING,\n"
                        + "i INT NOT NULL,\n"
                        + "t TIMESTAMP_LTZ(3),\n"
                        + "WATERMARK FOR t AS t + INTERVAL '1' SECOND,\n"
                        + "c AS UPPER(s))\n"
                        + "PARTITIONED BY (s)\n"
                        + "WITH ('connector' = 'confluent', 'confluent.kafka.topic' = 'SECRET')");
        tableEnv.executeSql(
                "CREATE TABLE `env-1`.`lkc-1`.`table2` (\n"
                        + "s STRING,\n"
                        + "headers MAP<STRING, STRING> METADATA VIRTUAL,\n"
                        + "ts TIMESTAMP(3) METADATA FROM 'timestamp'\n"
                        + ")\n"
                        + "COMMENT 'Hello'\n"
                        + "PARTITIONED BY (s)\n"
                        + "WITH ('connector' = 'confluent')");
        tableEnv.executeSql(
                "CREATE TABLE `env-1`.`lkc-2`.`table3` (\n"
                        + "s STRING PRIMARY KEY NOT ENFORCED,\n"
                        + "$rowtime TIMESTAMP_LTZ(3) NOT NULL METADATA VIRTUAL COMMENT 'SYSTEM'\n"
                        + ")\n"
                        + "WITH ('connector' = 'confluent')");
        tableEnv.executeSql(
                "CREATE TABLE `env-1`.`lkc-2`.`table4` (\n"
                        + "s1 STRING,\n"
                        + "s2 STRING,\n"
                        + "i INT,\n"
                        + "CONSTRAINT my_pk PRIMARY KEY (s1, s2) NOT ENFORCED\n"
                        + ")\n"
                        + "WITH ('connector' = 'confluent')");
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
    void testListDatabases() throws Exception {
        assertResult(
                "SELECT * FROM `INFORMATION_SCHEMA`.`DATABASES`",
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
    void testListTables() throws Exception {
        assertResult(
                "SELECT * FROM `env-1`.`INFORMATION_SCHEMA`.`TABLES` "
                        + "WHERE `TABLE_SCHEMA` <> 'INFORMATION_SCHEMA'",
                row(
                        "env-1",
                        "cat1",
                        "lkc-1",
                        "db1",
                        "table1",
                        "BASE TABLE",
                        // IS_DISTRIBUTED
                        "YES",
                        "HASH",
                        null,
                        // IS_WATERMARKED
                        "YES",
                        "t",
                        "`t` + INTERVAL '1' SECOND",
                        "NO",
                        null),
                row(
                        "env-1",
                        "cat1",
                        "lkc-1",
                        "db1",
                        "table2",
                        "BASE TABLE",
                        // IS_DISTRIBUTED
                        "YES",
                        "HASH",
                        null,
                        "NO",
                        null,
                        null,
                        null,
                        // COMMENT
                        "Hello"),
                row(
                        "env-1",
                        "cat1",
                        "lkc-2",
                        "db2",
                        "table3",
                        "BASE TABLE",
                        "NO",
                        null,
                        null,
                        "NO",
                        null,
                        null,
                        null,
                        null),
                row(
                        "env-1",
                        "cat1",
                        "lkc-2",
                        "db2",
                        "table4",
                        "BASE TABLE",
                        "NO",
                        null,
                        null,
                        "NO",
                        null,
                        null,
                        null,
                        null));
    }

    @Test
    void testListViews() throws Exception {
        // Testing one INFORMATION_SCHEMA table should be sufficient
        assertResult(
                "SELECT * FROM `env-1`.`INFORMATION_SCHEMA`.`TABLES` "
                        + "WHERE `TABLE_SCHEMA` = 'INFORMATION_SCHEMA' AND `TABLE_NAME` = 'SCHEMATA'",
                row(
                        "env-1",
                        "cat1",
                        "$information-schema",
                        "INFORMATION_SCHEMA",
                        "SCHEMATA",
                        "VIEW",
                        "NO",
                        null,
                        null,
                        "NO",
                        null,
                        null,
                        null,
                        "SYSTEM"));
    }

    @Test
    void testListTableOptions() throws Exception {
        assertResult(
                "SELECT * FROM `env-1`.`INFORMATION_SCHEMA`.`TABLE_OPTIONS` "
                        + "WHERE `TABLE_SCHEMA` <> 'INFORMATION_SCHEMA'",
                // The private option is correctly filtered out.
                row("env-1", "cat1", "lkc-1", "db1", "table1", "connector", "confluent"),
                row("env-1", "cat1", "lkc-1", "db1", "table2", "connector", "confluent"),
                row("env-1", "cat1", "lkc-2", "db2", "table3", "connector", "confluent"),
                row("env-1", "cat1", "lkc-2", "db2", "table4", "connector", "confluent"));
    }

    @Test
    void testListColumns() throws Exception {
        assertResult(
                "SELECT * FROM `env-1`.`INFORMATION_SCHEMA`.`COLUMNS` "
                        + "WHERE `TABLE_SCHEMA` <> 'INFORMATION_SCHEMA' AND `TABLE_NAME` = 'table1'",
                // physical column with STRING and distribution key
                row(
                        "env-1",
                        "cat1",
                        "lkc-1",
                        "db1",
                        "table1",
                        "s",
                        1,
                        "YES",
                        "VARCHAR",
                        "VARCHAR(2147483647)",
                        "NO",
                        "NO",
                        null,
                        "NO",
                        null,
                        "YES",
                        1,
                        null),
                // physical column with NOT NULL
                row(
                        "env-1",
                        "cat1",
                        "lkc-1",
                        "db1",
                        "table1",
                        "i",
                        2,
                        "NO",
                        "INTEGER",
                        "INT NOT NULL",
                        "NO",
                        "NO",
                        null,
                        "NO",
                        null,
                        "YES",
                        null,
                        null),
                // physical column with TIMESTAMP_LTZ
                row(
                        "env-1",
                        "cat1",
                        "lkc-1",
                        "db1",
                        "table1",
                        "t",
                        3,
                        "YES",
                        "TIMESTAMP_LTZ",
                        "TIMESTAMP(3) WITH LOCAL TIME ZONE",
                        "NO",
                        "NO",
                        null,
                        "NO",
                        null,
                        "YES",
                        null,
                        null),
                // computed column
                row(
                        "env-1",
                        "cat1",
                        "lkc-1",
                        "db1",
                        "table1",
                        "c",
                        4,
                        "YES",
                        "VARCHAR",
                        "VARCHAR(2147483647)",
                        "NO",
                        "YES",
                        "UPPER(`s`)",
                        "NO",
                        null,
                        "NO",
                        null,
                        null));
    }

    @Test
    void testListColumnsWithMetadata() throws Exception {
        assertResult(
                "SELECT `COLUMN_NAME`, `IS_HIDDEN`, `METADATA_KEY`, `IS_PERSISTED` "
                        + "FROM `env-1`.`INFORMATION_SCHEMA`.`COLUMNS` "
                        + "WHERE `TABLE_SCHEMA` <> 'INFORMATION_SCHEMA' AND `IS_METADATA` = 'YES'",
                // virtual metadata column
                row("headers", "NO", null, "NO"),
                // persisted metadata column with key
                row("ts", "NO", "timestamp", "YES"),
                // system column
                row("$rowtime", "YES", null, "NO"));
    }

    @Test
    void testListConstraints() throws Exception {
        assertResult(
                "SELECT * FROM `env-1`.`INFORMATION_SCHEMA`.`TABLE_CONSTRAINTS` "
                        + "WHERE `TABLE_SCHEMA` <> 'INFORMATION_SCHEMA'",
                // primary key not enforced with auto-generated name
                row(
                        "env-1",
                        "cat1",
                        "lkc-2",
                        "db2",
                        "PK_146",
                        "env-1",
                        "cat1",
                        "lkc-2",
                        "db2",
                        "table3",
                        "PRIMARY KEY",
                        "NO"),
                // primary key not enforced with name "my_pk"
                row(
                        "env-1",
                        "cat1",
                        "lkc-2",
                        "db2",
                        "my_pk",
                        "env-1",
                        "cat1",
                        "lkc-2",
                        "db2",
                        "table4",
                        "PRIMARY KEY",
                        "NO"));
    }

    @Test
    void testListKeyColumnUsage() throws Exception {
        assertResult(
                "SELECT * FROM `env-1`.`INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` "
                        + "WHERE `TABLE_SCHEMA` <> 'INFORMATION_SCHEMA'",
                row(
                        "env-1", "cat1", "lkc-2", "db2", "PK_146", "env-1", "cat1", "lkc-2", "db2",
                        "table3", "s", 1),
                row(
                        "env-1", "cat1", "lkc-2", "db2", "my_pk", "env-1", "cat1", "lkc-2", "db2",
                        "table4", "s1", 1),
                row(
                        "env-1", "cat1", "lkc-2", "db2", "my_pk", "env-1", "cat1", "lkc-2", "db2",
                        "table4", "s2", 2));
    }

    @Test
    void testUnsupportedBackgroundQuery() {
        assertThatThrownBy(
                        () ->
                                ResultPlanUtils.backgroundJob(
                                        tableEnv,
                                        "INSERT INTO simple_sink SELECT `SCHEMA_NAME` "
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
                        + "be accessed without providing the required column: CATALOG_ID");
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
