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
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;

import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        final QueryOperation queryOperation =
                tableEnv.sqlQuery("SELECT * FROM (VALUES (1), (2), (3))").getQueryOperation();

        final ForegroundResultPlan plan =
                INSTANCE.compileForegroundQuery(
                        tableEnv,
                        queryOperation,
                        (identifier, execNodeId) -> Collections.emptyMap());
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
        INSTANCE.configureEnvironment(tableEnv);

        final QueryOperation queryOperation =
                tableEnv.sqlQuery("SELECT NOW(), COUNT(*) FROM (VALUES (1), (2), (3))")
                        .getQueryOperation();

        assertThatThrownBy(
                        () ->
                                INSTANCE.compileForegroundQuery(
                                        tableEnv,
                                        queryOperation,
                                        (identifier, execNodeId) -> Collections.emptyMap()))
                .hasMessageContaining("can not satisfy the determinism requirement");
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

        final QueryOperation queryOperation =
                tableEnv.sqlQuery(
                                "SELECT SUM(amount) "
                                        + "FROM source "
                                        + "GROUP BY user, TUMBLE(ts, INTERVAL '5' SECOND)")
                        .getQueryOperation();

        assertThatThrownBy(
                        () ->
                                INSTANCE.compileForegroundQuery(
                                        tableEnv,
                                        queryOperation,
                                        (identifier, execNodeId) -> Collections.emptyMap()))
                .hasMessageContaining(
                        "SQL syntax that calls TUMBLE, HOP, and SESSION in the GROUP BY clause is "
                                + "not supported. Use table-valued function (TVF) syntax instead "
                                + "which is standard compliant.");
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
