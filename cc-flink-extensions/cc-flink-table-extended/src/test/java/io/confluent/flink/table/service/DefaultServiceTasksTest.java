/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;

import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.modules.core.CoreProxyModule;
import io.confluent.flink.table.service.ServiceTasks.Service;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultServiceTasks}. */
@Confluent
public class DefaultServiceTasksTest {

    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    @Test
    void testCompileForegroundQuery() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final ForegroundResultPlan plan =
                ResultPlanUtils.foregroundQuery(tableEnv, "SELECT * FROM (VALUES (1), (2), (3))");

        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    void testCompileBackgroundQueries() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final Schema schema = Schema.newBuilder().column("i", "INT").build();
        final Map<String, String> privateOptions =
                Collections.singletonMap("confluent.specific", "option");

        createConfluentCatalogTable(
                tableEnv,
                "source",
                schema,
                Collections.singletonMap("connector", "datagen"),
                privateOptions);

        createConfluentCatalogTable(
                tableEnv,
                "sink",
                schema,
                Collections.singletonMap("connector", "blackhole"),
                privateOptions);

        final List<Operation> operations =
                ((TableEnvironmentImpl) tableEnv)
                        .getPlanner()
                        .getParser()
                        .parse("INSERT INTO sink SELECT * FROM source");
        final SinkModifyOperation modifyOperation = (SinkModifyOperation) operations.get(0);
        assertThat(modifyOperation.getContextResolvedTable().getTable().getOptions())
                .doesNotContainKey("confluent.specific");

        final ConnectorOptionsProvider optionsProvider =
                (identifier, execNodeId) -> {
                    // execNodeId is omitted because it is not deterministic
                    return Collections.singletonMap(
                            "transactional-id", "my_" + identifier.getObjectName());
                };

        final BackgroundResultPlan plan =
                INSTANCE.compileBackgroundQueries(
                        tableEnv, Collections.singletonList(modifyOperation), optionsProvider);

        assertThat(plan.getCompiledPlan().replaceAll("[\\s\"]", ""))
                .contains(
                        "options:{connector:datagen,confluent.specific:option,transactional-id:my_source}")
                .contains(
                        "options:{connector:blackhole,confluent.specific:option,transactional-id:my_sink}");
    }

    @Test
    void testConfigurationNonDeterminism() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        assertThatThrownBy(
                        () ->
                                ResultPlanUtils.foregroundQuery(
                                        tableEnv,
                                        "SELECT NOW(), COUNT(*) FROM (VALUES (1), (2), (3))"))
                .hasMessageContaining("can not satisfy the determinism requirement");
    }

    @Test
    void testConfigurationSystemColumns() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), Collections.emptyMap(), Service.SQL_SERVICE);

        final Schema schema =
                Schema.newBuilder()
                        .column("i", DataTypes.INT())
                        .columnByMetadata("m_virtual", DataTypes.STRING(), true)
                        .columnByMetadata("m_persisted", DataTypes.STRING())
                        .build();
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "values");
        options.put("readable-metadata", "m_virtual:STRING,m_persisted:STRING");

        createConfluentCatalogTable(tableEnv, "source", schema, options, Collections.emptyMap());

        final QueryOperation queryOperation =
                tableEnv.sqlQuery("SELECT * FROM source").getQueryOperation();

        assertThat(queryOperation.getResolvedSchema().getColumnNames())
                .containsExactly("i", "m_persisted");
    }

    @Test
    void testConfigurationCoreProxyModule() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), Collections.emptyMap(), Service.SQL_SERVICE);

        assertThat(tableEnv.listFunctions()).hasSameElementsAs(CoreProxyModule.PUBLIC_LIST);
    }

    @Test
    void testConfigurationAiFunctionsModule() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.singletonMap("confluent.ai-functions.enabled", "true"),
                Service.SQL_SERVICE);

        assertThat(tableEnv.listFunctions().length)
                .isGreaterThan(CoreProxyModule.PUBLIC_LIST.size());
    }

    @Test
    void testUnsupportedGroupWindowSyntax() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        createConfluentCatalogTable(
                tableEnv,
                "source",
                Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("ts", "SOURCE_WATERMARK()")
                        .build(),
                Collections.singletonMap("connector", "datagen"),
                Collections.emptyMap());

        assertThatThrownBy(
                        () ->
                                ResultPlanUtils.foregroundQuery(
                                        tableEnv,
                                        "SELECT SUM(amount) "
                                                + "FROM source "
                                                + "GROUP BY user, TUMBLE(ts, INTERVAL '5' SECOND)"))
                .hasMessageContaining(
                        "SQL syntax that calls TUMBLE, HOP, and SESSION in the GROUP BY clause is "
                                + "not supported. Use table-valued function (TVF) syntax instead "
                                + "which is standard compliant.");
    }

    @Test
    void testConfiguration() {
        final Map<String, String> validPublicOptions = new HashMap<>();
        validPublicOptions.put("client.what-ever", "ANY_VALUE");
        validPublicOptions.put("sql.current-catalog", "my_cat");
        validPublicOptions.put("sql.current-database", "my_db");
        validPublicOptions.put("sql.state-ttl", "7 days");
        validPublicOptions.put("sql.local-time-zone", "Europe/Berlin");
        validPublicOptions.put("sql.tables.scan.idle-timeout", "5 min");
        validPublicOptions.put("sql.tables.scan.startup.mode", "latest-offset");
        validPublicOptions.put("sql.tables.scan.startup.timestamp-millis", "1001");
        validPublicOptions.put("sql.tables.scan.bounded.mode", "latest-offset");
        validPublicOptions.put("sql.tables.scan.bounded.timestamp-millis", "1002");

        final Map<String, String> validPrivateOptions = new HashMap<>();
        validPrivateOptions.put("confluent.ai-functions.enabled", "true");

        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnv.registerCatalog("my_cat", new GenericInMemoryCatalog("my_cat", "my_db"));

        final Map<String, String> resourceOptions =
                INSTANCE.configureEnvironment(
                        tableEnv, validPublicOptions, validPrivateOptions, Service.SQL_SERVICE);

        final Map<String, String> expectedResourceOptions = new HashMap<>();
        expectedResourceOptions.put("confluent.user.sql.current-catalog", "my_cat");
        expectedResourceOptions.put("confluent.user.sql.current-database", "my_db");
        expectedResourceOptions.put("confluent.user.sql.state-ttl", "7 days");
        expectedResourceOptions.put("confluent.user.sql.tables.scan.idle-timeout", "5 min");
        expectedResourceOptions.put("confluent.user.sql.local-time-zone", "Europe/Berlin");
        expectedResourceOptions.put("confluent.user.sql.tables.scan.startup.mode", "latest-offset");
        expectedResourceOptions.put(
                "confluent.user.sql.tables.scan.startup.timestamp-millis", "1001");
        expectedResourceOptions.put("confluent.user.sql.tables.scan.bounded.mode", "latest-offset");
        expectedResourceOptions.put(
                "confluent.user.sql.tables.scan.bounded.timestamp-millis", "1002");
        expectedResourceOptions.put("confluent.ai-functions.enabled", "true");

        assertThat(resourceOptions).isEqualTo(expectedResourceOptions);

        assertThat(tableEnv.getCurrentCatalog()).isEqualTo("my_cat");
        assertThat(tableEnv.getCurrentDatabase()).isEqualTo("my_db");
        assertThat(tableEnv.getConfig().getIdleStateRetention()).isEqualTo(Duration.ofDays(7));
        assertThat(tableEnv.getConfig().getLocalTimeZone()).isEqualTo(ZoneId.of("Europe/Berlin"));
        assertThat(tableEnv.getConfig().get(TABLE_EXEC_SOURCE_IDLE_TIMEOUT))
                .isEqualTo(Duration.ofMinutes(5));
    }

    @Test
    void testConfigurationValidation() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // Deprecated keys
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.singletonMap(
                        "catalog", TableConfigOptions.TABLE_CATALOG_NAME.defaultValue()),
                Collections.emptyMap(),
                Service.SQL_SERVICE);

        // Invalid values
        assertThatThrownBy(
                        () ->
                                INSTANCE.configureEnvironment(
                                        tableEnv,
                                        Collections.singletonMap("sql.state-ttl", "INVALID"),
                                        Collections.emptyMap(),
                                        Service.SQL_SERVICE))
                .hasMessageContaining("Invalid value for option 'sql.state-ttl'.");
        assertThatThrownBy(
                        () ->
                                INSTANCE.configureEnvironment(
                                        tableEnv,
                                        Collections.singletonMap(
                                                "sql.local-time-zone", "UTC-01:00"),
                                        Collections.emptyMap(),
                                        Service.SQL_SERVICE))
                .hasMessageContaining("Invalid value for option 'sql.local-time-zone'.");

        // Invalid key space
        assertThatThrownBy(
                        () ->
                                INSTANCE.configureEnvironment(
                                        tableEnv,
                                        Collections.singletonMap("does-not-exist", "42"),
                                        Collections.emptyMap(),
                                        Service.SQL_SERVICE))
                .hasMessageContaining(
                        "Unsupported configuration options found.\n"
                                + "\n"
                                + "Unsupported options:\n"
                                + "does-not-exist\n"
                                + "\n"
                                + "Supported options:\n"
                                + "sql.current-catalog\n"
                                + "sql.current-database\n"
                                + "sql.local-time-zone\n"
                                + "sql.state-ttl\n"
                                + "sql.tables.scan.bounded.mode\n"
                                + "sql.tables.scan.bounded.timestamp-millis\n"
                                + "sql.tables.scan.idle-timeout\n"
                                + "sql.tables.scan.startup.mode\n"
                                + "sql.tables.scan.startup.timestamp-millis");

        // Reserved catalog name
        assertThatThrownBy(
                        () ->
                                INSTANCE.configureEnvironment(
                                        tableEnv,
                                        Collections.singletonMap(
                                                "sql.current-catalog", "<UNKNOWN>"),
                                        Collections.emptyMap(),
                                        Service.SQL_SERVICE))
                .hasMessageContaining("Catalog name '<UNKNOWN>' is not allowed.");
    }

    private static void createConfluentCatalogTable(
            TableEnvironment tableEnv,
            String name,
            Schema schema,
            Map<String, String> publicOptions,
            Map<String, String> privateOptions)
            throws Exception {
        final Catalog catalog =
                tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                        .orElseThrow(IllegalArgumentException::new);
        catalog.createTable(
                new ObjectPath(tableEnv.getCurrentDatabase(), name),
                new ConfluentCatalogTable(
                        schema, null, Collections.emptyList(), publicOptions, privateOptions),
                false);
    }

    @Test
    void testShowCreateTable() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Map<String, String> publicOptions = new HashMap<>();

        publicOptions.put("key.format", "raw");
        publicOptions.put("connector", "confluent");
        publicOptions.put("changelog.mode", "append");
        publicOptions.put("scan.startup.mode", "earliest-offset");
        publicOptions.put("value.format", "raw");

        createConfluentCatalogTable(
                tableEnv,
                "source",
                Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("ts", "SOURCE_WATERMARK()")
                        .build(),
                publicOptions,
                Collections.emptyMap());
        final String sqlStmt = "SHOW CREATE TABLE source";
        TableResult result = tableEnv.executeSql(sqlStmt);
        String showCreateTableResult = result.collect().next().toString();
        assertThat(showCreateTableResult)
                .contains(
                        "CREATE TABLE `default_catalog`.`default_database`.`source` (\n"
                                + "  `user` VARCHAR(2147483647),\n"
                                + "  `amount` INT,\n"
                                + "  `ts` TIMESTAMP(3) WITH LOCAL TIME ZONE,\n"
                                + "  WATERMARK FOR `ts` AS `SOURCE_WATERMARK`()\n"
                                + ") WITH (\n"
                                + "  'changelog.mode' = 'append',\n"
                                + "  'connector' = 'confluent',\n"
                                + "  'key.format' = 'raw',\n"
                                + "  'scan.startup.mode' = 'earliest-offset',\n"
                                + "  'value.format' = 'raw'\n"
                                + ")");
    }
}
