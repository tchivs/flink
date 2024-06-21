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
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.modules.core.CoreProxyModule;
import io.confluent.flink.table.modules.ml.MLPredictFunction;
import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import io.confluent.flink.table.service.ServiceTasks.Service;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJob(tableEnv, "SELECT * FROM (VALUES (1), (2), (3))");

        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    void testCompileForegroundQueryPointInTime() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inBatchMode());
        ResultPlanUtils.createConfluentCatalogTable(
                tableEnv,
                "source",
                Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                        .build(),
                ImmutableMap.of("connector", "datagen", "number-of-rows", "10"));

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJob(
                        tableEnv, "SELECT SUM(amount) FROM source GROUP BY user");

        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    void testCompileBackgroundQueries() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final Schema schema = Schema.newBuilder().column("i", "INT").build();

        ResultPlanUtils.createConfluentCatalogTable(
                tableEnv,
                "source",
                schema,
                Map.ofEntries(
                        Map.entry("connector", "datagen"), Map.entry("rows-per-second", "10")));

        ResultPlanUtils.createConfluentCatalogTable(
                tableEnv, "sink", schema, Collections.singletonMap("connector", "blackhole"));

        final List<Operation> operations =
                ((TableEnvironmentImpl) tableEnv)
                        .getPlanner()
                        .getParser()
                        .parse("INSERT INTO sink SELECT * FROM source");
        final SinkModifyOperation modifyOperation = (SinkModifyOperation) operations.get(0);

        final ConnectorOptionsMutator optionsProvider =
                (identifier, execNodeId, tableOptions) -> {
                    // execNodeId is omitted because it is not deterministic
                    tableOptions.put("transactional-id", "my_" + identifier.getObjectName());

                    tableOptions.remove("rows-per-second");
                };

        final BackgroundJobResultPlan plan =
                INSTANCE.compileBackgroundQueries(
                        tableEnv, Collections.singletonList(modifyOperation), optionsProvider);

        assertThat(plan.getCompiledPlan().replaceAll("[\\s\"]", ""))
                .contains("options:{connector:datagen,transactional-id:my_source}")
                .contains("options:{connector:blackhole,transactional-id:my_sink}");
    }

    @Test
    void testConfigurationNonDeterminism() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        assertThatThrownBy(
                        () ->
                                ResultPlanUtils.foregroundJob(
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

        ResultPlanUtils.createConfluentCatalogTable(tableEnv, "source", schema, options);

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
    void testConfigureMLFunctionModel() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.singletonMap("confluent.ml-functions.enabled", "true"),
                Service.SQL_SERVICE);

        String[] functions = tableEnv.listFunctions();
        assertThat(Arrays.asList(functions).contains(MLPredictFunction.NAME)).isTrue();
    }

    @Test
    void testRegisterMLPredictFunction() throws JsonProcessingException {
        // Create unresolved schema. DataType should be UnresolvedDataType
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "INT").build();

        final ResolvedSchema resolvedInputSchema =
                ResolvedSchema.physical(
                        Collections.singletonList("input"),
                        Collections.singletonList(DataTypes.STRING()));
        final ResolvedSchema resovledOutputSchema =
                ResolvedSchema.physical(
                        Collections.singletonList("output"),
                        Collections.singletonList(DataTypes.INT()));

        final ResolvedCatalogModel resolvedCatalogModel =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                inputSchema,
                                outputSchema,
                                ImmutableMap.of("provider", "openai", "task", "CLASSIFICATION"),
                                ""),
                        resolvedInputSchema,
                        resovledOutputSchema);

        final TableEnvironmentImpl tableEnv =
                (TableEnvironmentImpl)
                        TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final ObjectMapper mapper = new ObjectMapper();
        final String serializedModel =
                mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(resolvedCatalogModel.toProperties());

        ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of("my_catalog", "my_db", "my_model_ML_PREDICT");
        tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog", "my_db"));
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.singletonMap(objectIdentifier.toString(), serializedModel),
                Service.JOB_SUBMISSION_SERVICE);

        tableEnv.useCatalog("my_catalog");
        tableEnv.useDatabase("my_db");
        final String[] udfs = tableEnv.listUserDefinedFunctions();
        assertThat(Arrays.asList(udfs).contains("my_model_ml_predict")).isTrue();
    }

    @Test
    void testRegisterMLEvaluateFunction() throws JsonProcessingException {
        // Create unresolved schema. DataType should be UnresolvedDataType
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "INT").build();

        final ResolvedSchema resolvedInputSchema =
                ResolvedSchema.physical(
                        Collections.singletonList("input"),
                        Collections.singletonList(DataTypes.STRING()));
        final ResolvedSchema resovledOutputSchema =
                ResolvedSchema.physical(
                        Collections.singletonList("output"),
                        Collections.singletonList(DataTypes.INT()));

        final ResolvedCatalogModel resolvedCatalogModel =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                inputSchema,
                                outputSchema,
                                ImmutableMap.of("provider", "openai", "task", "CLASSIFICATION"),
                                ""),
                        resolvedInputSchema,
                        resovledOutputSchema);

        final TableEnvironmentImpl tableEnv =
                (TableEnvironmentImpl)
                        TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final ObjectMapper mapper = new ObjectMapper();
        final String serializedModel =
                mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(resolvedCatalogModel.toProperties());

        ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of("my_catalog", "my_db", "my_model_ML_EVALUATE");
        tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog", "my_db"));
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.singletonMap(objectIdentifier.toString(), serializedModel),
                Service.JOB_SUBMISSION_SERVICE);

        tableEnv.useCatalog("my_catalog");
        tableEnv.useDatabase("my_db");
        final String[] udfs = tableEnv.listUserDefinedFunctions();
        assertThat(Arrays.asList(udfs).contains("my_model_ml_evaluate")).isTrue();
    }

    @Test
    void testRegisterMLEvaluateAllFunction() throws JsonProcessingException {
        // Create unresolved schema. DataType should be UnresolvedDataType
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "INT").build();

        final ResolvedSchema resolvedInputSchema =
                ResolvedSchema.physical(
                        Collections.singletonList("input"),
                        Collections.singletonList(DataTypes.STRING()));
        final ResolvedSchema resovledOutputSchema =
                ResolvedSchema.physical(
                        Collections.singletonList("output"),
                        Collections.singletonList(DataTypes.INT()));

        final ResolvedCatalogModel resolvedCatalogModel =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                inputSchema,
                                outputSchema,
                                ImmutableMap.of("provider", "openai", "task", "CLASSIFICATION"),
                                ""),
                        resolvedInputSchema,
                        resovledOutputSchema);

        final TableEnvironmentImpl tableEnv =
                (TableEnvironmentImpl)
                        TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<Boolean, Map<String, String>>> modelVersion = new HashMap<>();
        modelVersion.put("1", ImmutableMap.of(true, resolvedCatalogModel.toProperties()));
        ObjectIdentifier objectIdentifier =
                ObjectIdentifier.of("my_catalog", "my_db", "my_model_ML_EVALUATE_ALL");
        tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog", "my_db"));

        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.singletonMap(
                        objectIdentifier.toString(),
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(modelVersion)),
                Service.JOB_SUBMISSION_SERVICE);

        tableEnv.useCatalog("my_catalog");
        tableEnv.useDatabase("my_db");
        final String[] udfs = tableEnv.listUserDefinedFunctions();
        assertThat(Arrays.asList(udfs).contains("my_model_ml_evaluate_all")).isTrue();
    }

    @Test
    void testRegisterAllMLFunctions() throws JsonProcessingException {
        // Create unresolved schema. DataType should be UnresolvedDataType
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "INT").build();

        final ResolvedSchema resolvedInputSchema =
                ResolvedSchema.physical(
                        Collections.singletonList("input"),
                        Collections.singletonList(DataTypes.STRING()));
        final ResolvedSchema resovledOutputSchema =
                ResolvedSchema.physical(
                        Collections.singletonList("output"),
                        Collections.singletonList(DataTypes.INT()));

        final ResolvedCatalogModel resolvedCatalogModel =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                inputSchema,
                                outputSchema,
                                ImmutableMap.of("provider", "openai", "task", "CLASSIFICATION"),
                                ""),
                        resolvedInputSchema,
                        resovledOutputSchema);

        final TableEnvironmentImpl tableEnv =
                (TableEnvironmentImpl)
                        TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final ObjectMapper mapper = new ObjectMapper();
        Map<String, Map<Boolean, Map<String, String>>> modelVersion = new HashMap<>();
        modelVersion.put("1", ImmutableMap.of(true, resolvedCatalogModel.toProperties()));
        ObjectIdentifier evalAllObjectIdentifier =
                ObjectIdentifier.of("my_catalog", "my_db", "my_model_ML_EVAlUATE_ALL");

        final String serializedModel =
                mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(resolvedCatalogModel.toProperties());

        ObjectIdentifier predictObjectIdentifier =
                ObjectIdentifier.of("my_catalog", "my_db", "my_model_ML_PREdICT");

        ObjectIdentifier evaluateObjectIdentifier =
                ObjectIdentifier.of("my_catalog", "my_db", "my_model_ML_EVALuATE");

        tableEnv.registerCatalog("my_catalog", new GenericInMemoryCatalog("my_catalog", "my_db"));

        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                ImmutableMap.of(
                        evalAllObjectIdentifier.toString(),
                        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(modelVersion),
                        predictObjectIdentifier.toString(),
                        serializedModel,
                        evaluateObjectIdentifier.toString(),
                        serializedModel),
                Service.JOB_SUBMISSION_SERVICE);

        tableEnv.useCatalog("my_catalog");
        tableEnv.useDatabase("my_db");
        final String[] udfs = tableEnv.listUserDefinedFunctions();
        assertThat(new HashSet<>(Arrays.asList(udfs)))
                .isEqualTo(
                        ImmutableSet.of(
                                "my_model_ml_evaluate_all",
                                "my_model_ml_predict",
                                "my_model_ml_evaluate"));
    }

    @Test
    void testUnsupportedGroupWindowSyntax() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        ResultPlanUtils.createConfluentCatalogTable(
                tableEnv,
                "source",
                Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("ts", "SOURCE_WATERMARK()")
                        .build(),
                Collections.singletonMap("connector", "datagen"));

        assertThatThrownBy(
                        () ->
                                ResultPlanUtils.foregroundJob(
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
        validPublicOptions.put("sql.secrets.openai", "api-key-secret");
        validPublicOptions.put("sql.secrets.remote", "remote-api-key-secret");

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
        expectedResourceOptions.put("confluent.user.sql.secrets.openai", "api-key-secret");
        expectedResourceOptions.put("confluent.user.sql.secrets.remote", "remote-api-key-secret");
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
    void testDryRun() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.singletonMap(ServiceTasksOptions.SQL_DRY_RUN.key(), "true"),
                Collections.emptyMap(),
                Service.SQL_SERVICE);

        // Invalid values
        assertThatThrownBy(
                        () ->
                                INSTANCE.configureEnvironment(
                                        tableEnv,
                                        Collections.singletonMap(
                                                ServiceTasksOptions.SQL_DRY_RUN.key(), "true"),
                                        Collections.emptyMap(),
                                        Service.JOB_SUBMISSION_SERVICE))
                .hasMessageContaining(
                        "Statement submitted for a dry run. It should never reach the JSS.");
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
                                + "sql.dry-run\n"
                                + "sql.local-time-zone\n"
                                + "sql.state-ttl\n"
                                + "sql.tables.scan.bounded.mode\n"
                                + "sql.tables.scan.bounded.timestamp-millis\n"
                                + "sql.tables.scan.idle-timeout\n"
                                + "sql.tables.scan.startup.mode\n"
                                + "sql.tables.scan.startup.timestamp-millis");

        // Early access options should not be shown in error messages
        assertThatThrownBy(
                        () ->
                                INSTANCE.configureEnvironment(
                                        tableEnv,
                                        Collections.singletonMap("does-not-exist", "42"),
                                        Collections.emptyMap(),
                                        Service.SQL_SERVICE))
                .hasMessageNotContaining("sql.secrets");

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

    @Test
    void testShowCreateTable() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Map<String, String> options = new HashMap<>();

        options.put("key.format", "raw");
        options.put("connector", "confluent");
        options.put("changelog.mode", "append");
        options.put("scan.startup.mode", "earliest-offset");
        options.put("value.format", "raw");

        ResultPlanUtils.createConfluentCatalogTable(
                tableEnv,
                "source",
                Schema.newBuilder()
                        .column("user", DataTypes.STRING())
                        .column("amount", DataTypes.INT())
                        .column("ts", DataTypes.TIMESTAMP_LTZ(3))
                        .watermark("ts", "SOURCE_WATERMARK()")
                        .build(),
                options);
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
