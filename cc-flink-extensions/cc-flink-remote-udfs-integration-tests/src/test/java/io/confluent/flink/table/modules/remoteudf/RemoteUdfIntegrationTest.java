/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCacheImpl;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTask;
import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTaskStatus;
import io.confluent.flink.apiserver.client.model.FlinkV1Job;
import io.confluent.flink.table.modules.remoteudf.mock.MockedFunctionWithTypes;
import io.confluent.flink.table.modules.remoteudf.mock.MockedUdfGateway;
import io.confluent.flink.table.modules.remoteudf.testcontainers.ApiServerContainer;
import io.confluent.flink.table.modules.remoteudf.util.ApiServerContainerUtils;
import io.confluent.flink.table.modules.remoteudf.util.TestUtils;
import io.confluent.flink.table.modules.remoteudf.utils.NamesGenerator;
import io.confluent.flink.udf.adapter.api.AdapterOptions;
import io.confluent.flink.udf.adapter.api.OpenPayload;
import io.confluent.flink.udf.adapter.api.RemoteUdfSpec;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.BYTES_FROM_UDF_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.BYTES_TO_UDF_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.DEPROVISIONS_MS_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.DEPROVISIONS_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.INVOCATION_FAILURES_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.INVOCATION_MS_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.INVOCATION_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.INVOCATION_SUCCESSES_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.METRIC_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.PROVISIONS_MS_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.PROVISIONS_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_APISERVER;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_ASYNC_ENABLED;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_BATCH_ENABLED;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_BATCH_SIZE;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_BATCH_WAIT_TIME_MS;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.JOB_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfRuntime.AUTH_METADATA;
import static io.confluent.flink.table.modules.remoteudf.utils.ApiServerUtils.LABEL_JOB_ID;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests collection for Remote UDF functionality including Compute Gateway and ApiServer
 * connections for UdfTask invocation and creation respectively.
 */
public class RemoteUdfIntegrationTest {
    private static final InMemoryReporter REPORTER = InMemoryReporter.createWithRetainedMetrics();

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setConfiguration(REPORTER.addToConfiguration(new Configuration()))
                            .build());

    private static final String CELL = String.format("cell-%s", NamesGenerator.nextRandomName());
    private static final String TEST_ORG = String.format("org-%s", NamesGenerator.nextRandomName());
    private static final String TEST_ENV = String.format("env-%s", NamesGenerator.nextRandomName());
    private static final String TEST_CP =
            String.format("compute-pool-%s", NamesGenerator.nextRandomName());
    private static final String TEST_JOB_NAME =
            String.format("job-%s", NamesGenerator.nextRandomName());

    private static final String SIMPLE_QUERY = "SELECT cat1.db1.remote1(1, 'test', 4);";
    private static final String NULL_QUERY = "SELECT cat1.db1.remote1(null, null, 4);";
    private static final String GW_HOST = "localhost";
    private static final int GW_PORT = 51000;
    private Server gatewayServer;
    private ApiServerContainer apiServerContainer;
    private ScheduledExecutorService executorService;

    private static MockedFunctionWithTypes[] testFunctionMeta =
            new MockedFunctionWithTypes[] {
                new MockedFunctionWithTypes(
                        "remote1",
                        ImmutableList.of(new String[] {"INT", "STRING", "INT"}),
                        ImmutableList.of("STRING")),
                new MockedFunctionWithTypes(
                        "remote2", ImmutableList.of(new String[] {"INT"}), ImmutableList.of("INT")),
                new MockedFunctionWithTypes(
                        "error",
                        ImmutableList.of(new String[] {"INT", "STRING", "INT"}),
                        ImmutableList.of("BIGINT"))
            };

    private static class UdfTaskCallable implements Runnable {
        final ApiServerContainer apiServerContainer;
        private final MockedUdfGateway testUdfGateway;

        UdfTaskCallable(
                final ApiServerContainer apiServerContainer,
                final MockedUdfGateway testUdfGateway) {
            this.apiServerContainer = apiServerContainer;
            this.testUdfGateway = testUdfGateway;
        }

        public void run() {
            try {
                Collection<ComputeV1FlinkUdfTask> udfTasks =
                        ApiServerContainerUtils.listUdfTasksWithStatus(
                                apiServerContainer, TEST_ORG, TEST_ENV, "Pending");
                if (!udfTasks.isEmpty()) {
                    ComputeV1FlinkUdfTask udfTask = udfTasks.iterator().next();
                    // Validate the udf task
                    Map<String, String> labels = udfTask.getMetadata().getLabels();
                    Assertions.assertFalse(labels.get(LABEL_JOB_ID).isEmpty());
                    // Validate Job Owner Reference
                    FlinkV1Job job =
                            ApiServerContainerUtils.getJob(
                                    apiServerContainer, TEST_ORG, TEST_ENV, TEST_JOB_NAME);
                    if (udfTask.getMetadata().getOwnerReferences() == null) {
                        // The owner reference hasn't yet been updated, so wait.
                        return;
                    }
                    Assertions.assertFalse(udfTask.getMetadata().getOwnerReferences().isEmpty());
                    Assertions.assertEquals(
                            udfTask.getMetadata().getOwnerReferences().get(0).getName(),
                            job.getMetadata().getName());

                    // Make sure metadata from Payload is properly propagated
                    OpenPayload open =
                            OpenPayload.open(
                                    udfTask.getSpec().getEntryPoint().getOpenPayload(),
                                    Thread.currentThread().getContextClassLoader());
                    RemoteUdfSpec udfSpec = open.getRemoteUdfSpec();
                    Configuration configuration = open.getConfiguration();
                    testUdfGateway.registerUdfSpec(
                            udfSpec,
                            configuration.get(AdapterOptions.ADAPTER_HANDLER_BATCH_ENABLED));

                    Assertions.assertEquals(
                            123, configuration.get(AdapterOptions.ADAPTER_PARALLELISM));

                    // Set to Running to move to GW invocation
                    udfTask.getStatus().setPhase(ComputeV1FlinkUdfTaskStatus.PhaseEnum.RUNNING);
                    udfTask.getStatus().getEndpoint().setHost(GW_HOST);
                    udfTask.getStatus().getEndpoint().setPort(GW_PORT);
                    apiServerContainer
                            .getComputeV1Api()
                            .updateComputeV1FlinkUdfTaskStatus(
                                    TEST_ENV, udfTask.getMetadata().getName(), TEST_ORG, udfTask);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @BeforeEach
    public void before() throws Exception {
        MockedUdfGateway testUdfGateway = new MockedUdfGateway();

        KeyPair keyPair = TestUtils.createKeyPair();
        Certificate x509 = TestUtils.createSelfSignedCertificate(keyPair);

        // Convert both key and cert to InputStream PEM format
        InputStream keySteam =
                new ByteArrayInputStream(
                        TestUtils.convertToBase64PEM(keyPair.getPrivate()).getBytes());
        InputStream certSteam =
                new ByteArrayInputStream(TestUtils.convertToBase64PEM(x509).getBytes());

        // Use the above to start a TSL enabled server endpoint
        SslContext sslContext = GrpcSslContexts.forServer(certSteam, keySteam).build();
        gatewayServer =
                NettyServerBuilder.forAddress(new InetSocketAddress(GW_HOST, GW_PORT))
                        .addService(testUdfGateway)
                        .sslContext(sslContext)
                        .intercept(new AuthServerInterceptor())
                        .build()
                        .start();

        apiServerContainer = new ApiServerContainer();
        apiServerContainer.start();

        ApiServerContainerUtils.createTestEnvAndOrg(apiServerContainer, TEST_ORG, TEST_ENV);
        ApiServerContainerUtils.createComputePool(
                apiServerContainer, CELL, TEST_ORG, TEST_ENV, TEST_CP);
        ApiServerContainerUtils.createJob(
                apiServerContainer, CELL, TEST_ORG, TEST_ENV, TEST_CP, TEST_JOB_NAME);
        Assertions.assertNotNull(
                ApiServerContainerUtils.getJob(
                        apiServerContainer, TEST_ORG, TEST_ENV, TEST_JOB_NAME));

        // Periodic service that marks UdfTask ready with metadata
        executorService = Executors.newSingleThreadScheduledExecutor();
        // Shutdown on completion
        executorService.scheduleAtFixedRate(
                new UdfTaskCallable(apiServerContainer, testUdfGateway),
                100,
                100,
                TimeUnit.MILLISECONDS);
    }

    private void testRemoteUdfGatewayInternal(
            boolean jss,
            boolean async,
            boolean batch,
            String query,
            Consumer<List<Row>> responseValidation,
            Consumer<Map<String, Metric>> metricsValidation)
            throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(JOB_NAME.key(), TEST_JOB_NAME);
        confMap.put(CONFLUENT_REMOTE_UDF_APISERVER.key(), apiServerContainer.getHostAddress());
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID.key(), "cpp-udf-shim");
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID.key(), "ver-udf-shim-1");
        confMap.put(CONFLUENT_REMOTE_UDF_ASYNC_ENABLED.key(), Boolean.toString(async));
        confMap.put(CONFLUENT_REMOTE_UDF_BATCH_ENABLED.key(), Boolean.toString(batch));
        confMap.put(AdapterOptions.ADAPTER_PARALLELISM.key(), Integer.toString(123));
        final TableEnvironment tEnv =
                jss
                        ? TestUtils.getJssTableEnvironment(
                                TEST_ORG, TEST_ENV, confMap, testFunctionMeta)
                        : TestUtils.getSqlServiceTableEnvironment(
                                TEST_ORG, TEST_ENV, confMap, testFunctionMeta, true, false);
        TableResult result = tEnv.executeSql(query);
        JobID jobID = result.getJobClient().get().getJobID();
        createCredentialsFor(jobID);
        final List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> collect = result.collect()) {
            collect.forEachRemaining(results::add);
        }
        responseValidation.accept(results);

        Collection<ComputeV1FlinkUdfTask> udfTasks =
                ApiServerContainerUtils.listUdfTasksWithStatus(
                        apiServerContainer, TEST_ORG, TEST_ENV, "Running");
        Assertions.assertEquals(0, udfTasks.size());

        Optional<MetricGroup> group =
                REPORTER.findGroups(METRIC_NAME).stream()
                        .filter(
                                g ->
                                        g.getAllVariables()
                                                .get(ScopeFormat.SCOPE_JOB_ID)
                                                .equals(jobID.toHexString()))
                        .findFirst();
        Assertions.assertTrue(group.isPresent());
        Map<String, Metric> metrics = REPORTER.getMetricsByGroup(group.get());
        metricsValidation.accept(metrics);
    }

    private void testRemoteUdfGatewayInternalBatch(boolean jss) throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(JOB_NAME.key(), TEST_JOB_NAME);
        confMap.put(CONFLUENT_REMOTE_UDF_APISERVER.key(), apiServerContainer.getHostAddress());
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID.key(), "cpp-udf-shim");
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID.key(), "ver-udf-shim-1");
        confMap.put(CONFLUENT_REMOTE_UDF_BATCH_ENABLED.key(), Boolean.toString(true));
        confMap.put(AdapterOptions.ADAPTER_PARALLELISM.key(), Integer.toString(123));
        confMap.put(CONFLUENT_REMOTE_UDF_BATCH_SIZE.key(), "500");
        confMap.put(CONFLUENT_REMOTE_UDF_BATCH_WAIT_TIME_MS.key(), "10000");
        final TableEnvironment tEnv =
                jss
                        ? TestUtils.getJssTableEnvironment(
                                TEST_ORG, TEST_ENV, confMap, testFunctionMeta)
                        : TestUtils.getSqlServiceTableEnvironment(
                                TEST_ORG, TEST_ENV, confMap, testFunctionMeta, true, false);
        tEnv.getConfig().set(TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY, 500);
        tEnv.executeSql(
                "CREATE TABLE Source ("
                        + "    f INT"
                        + ") WITH ('connector' = 'datagen',"
                        + " 'number-of-rows' = '10000',"
                        + " 'fields.f.kind' = 'sequence',"
                        + " 'fields.f.start' = '0',"
                        + " 'fields.f.end' = '9999');");

        TableResult result = tEnv.executeSql("SELECT cat1.db1.remote2(f) from Source;");

        JobID jobID = result.getJobClient().get().getJobID();
        createCredentialsFor(jobID);
        final List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> collect = result.collect()) {
            collect.forEachRemaining(results::add);
        }
        Assertions.assertEquals(10000, results.size());
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 500; j++) {
                Row row = results.get(i * 500 + j);
                Assertions.assertEquals(j, row.getField(0));
            }
        }

        Collection<ComputeV1FlinkUdfTask> udfTasks =
                ApiServerContainerUtils.listUdfTasksWithStatus(
                        apiServerContainer, TEST_ORG, TEST_ENV, "Running");
        Assertions.assertEquals(0, udfTasks.size());

        Optional<MetricGroup> group =
                REPORTER.findGroups(METRIC_NAME).stream()
                        .filter(
                                g ->
                                        g.getAllVariables()
                                                .get(ScopeFormat.SCOPE_JOB_ID)
                                                .equals(jobID.toHexString()))
                        .findFirst();
        Assertions.assertTrue(group.isPresent());
        Map<String, Metric> metrics = REPORTER.getMetricsByGroup(group.get());
        Assertions.assertEquals(10000, getCounter(metrics, INVOCATION_NAME));
        Assertions.assertTrue(getGauge(metrics, INVOCATION_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, INVOCATION_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(10000, getCounter(metrics, INVOCATION_SUCCESSES_NAME));
        Assertions.assertEquals(0, getCounter(metrics, INVOCATION_FAILURES_NAME));
        Assertions.assertEquals(1, getCounter(metrics, PROVISIONS_NAME));
        Assertions.assertTrue(getGauge(metrics, PROVISIONS_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, PROVISIONS_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(1, getCounter(metrics, DEPROVISIONS_NAME));
        Assertions.assertTrue(getGauge(metrics, DEPROVISIONS_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, DEPROVISIONS_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(50080, getCounter(metrics, BYTES_TO_UDF_NAME));
        Assertions.assertEquals(50080, getCounter(metrics, BYTES_FROM_UDF_NAME));
    }

    private void validateSimpleResponse(final List<Row> results) {
        Assertions.assertEquals(1, results.size());
        Row row = results.get(0);
        Assertions.assertEquals("str:[1, test, 4]", row.getField(0));
    }

    private void validateNullResponse(final List<Row> results) {
        Assertions.assertEquals(1, results.size());
        Row row = results.get(0);
        Assertions.assertEquals("str:[null, null, 4]", row.getField(0));
    }

    private void validateStreamingSimpleMetrics(Map<String, Metric> metrics) {
        validateSimpleMetrics(metrics, 19, 21);
    }

    private void validateBatchSimpleMetrics(Map<String, Metric> metrics) {
        validateSimpleMetrics(metrics, 23, 25);
    }

    private void validateStreamingNullMetrics(Map<String, Metric> metrics) {
        validateSimpleMetrics(metrics, 7, 24);
    }

    private void validateBatchNullMetrics(Map<String, Metric> metrics) {
        validateSimpleMetrics(metrics, 11, 28);
    }

    private void validateSimpleMetrics(Map<String, Metric> metrics, int toBytes, int fromBytes) {
        Assertions.assertEquals(1, getCounter(metrics, INVOCATION_NAME));
        Assertions.assertTrue(getGauge(metrics, INVOCATION_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, INVOCATION_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(1, getCounter(metrics, INVOCATION_SUCCESSES_NAME));
        Assertions.assertEquals(0, getCounter(metrics, INVOCATION_FAILURES_NAME));
        Assertions.assertEquals(1, getCounter(metrics, PROVISIONS_NAME));
        Assertions.assertTrue(getGauge(metrics, PROVISIONS_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, PROVISIONS_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(1, getCounter(metrics, DEPROVISIONS_NAME));
        Assertions.assertTrue(getGauge(metrics, DEPROVISIONS_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, DEPROVISIONS_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(toBytes, getCounter(metrics, BYTES_TO_UDF_NAME));
        Assertions.assertEquals(fromBytes, getCounter(metrics, BYTES_FROM_UDF_NAME));
    }

    private void validateStreamingErrorMetrics(Map<String, Metric> metrics) {
        validateErrorMetrics(metrics, 19, 0);
    }

    private void validateBatchErrorMetrics(Map<String, Metric> metrics) {
        validateErrorMetrics(metrics, 23, 0);
    }

    private void validateErrorMetrics(Map<String, Metric> metrics, int toBytes, int fromBytes) {
        Assertions.assertEquals(1, getCounter(metrics, INVOCATION_NAME));
        Assertions.assertTrue(getGauge(metrics, INVOCATION_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, INVOCATION_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(0, getCounter(metrics, INVOCATION_SUCCESSES_NAME));
        Assertions.assertEquals(1, getCounter(metrics, INVOCATION_FAILURES_NAME));
        Assertions.assertEquals(1, getCounter(metrics, PROVISIONS_NAME));
        Assertions.assertTrue(getGauge(metrics, PROVISIONS_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, PROVISIONS_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(1, getCounter(metrics, DEPROVISIONS_NAME));
        Assertions.assertTrue(getGauge(metrics, DEPROVISIONS_MS_NAME).isPresent());
        Assertions.assertTrue((Long) getGauge(metrics, DEPROVISIONS_MS_NAME).get().getValue() > 0);
        Assertions.assertEquals(toBytes, getCounter(metrics, BYTES_TO_UDF_NAME));
        Assertions.assertEquals(fromBytes, getCounter(metrics, BYTES_FROM_UDF_NAME));
    }

    private void createCredentialsFor(JobID jobID) {
        KafkaCredentialsCacheImpl.INSTANCE.onNewCredentialsObtained(
                ImmutableMap.of(jobID, new KafkaCredentials("abc", Optional.of("udf_def"))));
    }

    @Test
    public void testRemoteUdfGateway() throws Exception {
        testRemoteUdfGatewayInternal(
                false,
                false,
                false,
                SIMPLE_QUERY,
                this::validateSimpleResponse,
                this::validateStreamingSimpleMetrics);
    }

    @Test
    public void testRemoteUdfGateway_jss() throws Exception {
        testRemoteUdfGatewayInternal(
                true,
                false,
                false,
                SIMPLE_QUERY,
                this::validateSimpleResponse,
                this::validateStreamingSimpleMetrics);
    }

    @Test
    public void testRemoteUdfGatewayAsync() throws Exception {
        testRemoteUdfGatewayInternal(
                false,
                true,
                false,
                SIMPLE_QUERY,
                this::validateSimpleResponse,
                this::validateStreamingSimpleMetrics);
    }

    @Test
    public void testRemoteUdfGatewayAsync_jss() throws Exception {
        testRemoteUdfGatewayInternal(
                true,
                true,
                false,
                SIMPLE_QUERY,
                this::validateSimpleResponse,
                this::validateStreamingSimpleMetrics);
    }

    @Test
    public void testRemoteUdfGatewayBatch() throws Exception {
        testRemoteUdfGatewayInternal(
                false,
                false,
                true,
                SIMPLE_QUERY,
                this::validateSimpleResponse,
                this::validateBatchSimpleMetrics);
    }

    @Test
    public void testRemoteUdfGatewayBatch_jss() throws Exception {
        testRemoteUdfGatewayInternal(
                true,
                false,
                true,
                SIMPLE_QUERY,
                this::validateSimpleResponse,
                this::validateBatchSimpleMetrics);
    }

    @Test
    public void testRemoteUdfGateway_largeBatch() throws Exception {
        testRemoteUdfGatewayInternalBatch(false);
    }

    @Test
    public void testRemoteUdfGateway_null() throws Exception {
        testRemoteUdfGatewayInternal(
                false,
                false,
                false,
                NULL_QUERY,
                this::validateNullResponse,
                this::validateStreamingNullMetrics);
    }

    @Test
    public void testRemoteUdfGateway_AsyncNull() throws Exception {
        testRemoteUdfGatewayInternal(
                false,
                true,
                false,
                NULL_QUERY,
                this::validateNullResponse,
                this::validateStreamingNullMetrics);
    }

    @Test
    public void testRemoteUdfGateway_BatchNull() throws Exception {
        testRemoteUdfGatewayInternal(
                false,
                false,
                true,
                NULL_QUERY,
                this::validateNullResponse,
                this::validateBatchNullMetrics);
    }

    @Test
    public void testRemoteUdfApiServerNotConfigured() {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(JOB_NAME.key(), TEST_JOB_NAME);
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID.key(), "cpp-udf-shim");
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID.key(), "ver-udf-shim-1");
        final TableEnvironment tableEnv =
                TestUtils.getSqlServiceTableEnvironment(
                        TEST_ORG, TEST_ENV, confMap, testFunctionMeta, true, false);
        assertThatThrownBy(
                        () -> {
                            TableResult result =
                                    tableEnv.executeSql("SELECT cat1.db1.remote1(1, 'test', 4)");
                            try (CloseableIterator<Row> iter = result.collect()) {
                                iter.forEachRemaining((x) -> {});
                            }
                        })
                .hasStackTraceContaining("ApiServer target not configured");
    }

    @Test
    public void testRemoteUdfGateway_error() {
        testRemoteUdfGatewayError(false, false, this::validateStreamingErrorMetrics);
    }

    @Test
    public void testRemoteUdfGatewayAsync_error() {
        testRemoteUdfGatewayError(true, false, this::validateStreamingErrorMetrics);
    }

    @Test
    public void testRemoteUdfGatewayBatch_error() {
        testRemoteUdfGatewayError(true, true, this::validateBatchErrorMetrics);
    }

    private void testRemoteUdfGatewayError(
            boolean async, boolean batch, Consumer<Map<String, Metric>> metricsValidation) {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(JOB_NAME.key(), TEST_JOB_NAME);
        confMap.put(CONFLUENT_REMOTE_UDF_APISERVER.key(), apiServerContainer.getHostAddress());
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID.key(), "cpp-udf-shim");
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID.key(), "ver-udf-shim-1");
        confMap.put(CONFLUENT_REMOTE_UDF_ASYNC_ENABLED.key(), Boolean.toString(async));
        confMap.put(CONFLUENT_REMOTE_UDF_BATCH_ENABLED.key(), Boolean.toString(batch));
        confMap.put(AdapterOptions.ADAPTER_PARALLELISM.key(), Integer.toString(123));
        final TableEnvironment tEnv =
                TestUtils.getSqlServiceTableEnvironment(
                        TEST_ORG, TEST_ENV, confMap, testFunctionMeta, true, false);
        TableResult result = tEnv.executeSql("SELECT cat1.db1.error(1, 'test', 4);");
        JobID jobID = result.getJobClient().get().getJobID();
        createCredentialsFor(jobID);
        ExecutionException e = Assertions.assertThrows(ExecutionException.class, result::await);
        assertCause("Unknown return type BIGINT", e);
        Optional<MetricGroup> group =
                REPORTER.findGroups(METRIC_NAME).stream()
                        .filter(
                                g ->
                                        g.getAllVariables()
                                                .get(ScopeFormat.SCOPE_JOB_ID)
                                                .equals(jobID.toHexString()))
                        .findFirst();
        Assertions.assertTrue(group.isPresent());
        Map<String, Metric> metrics = REPORTER.getMetricsByGroup(group.get());
        metricsValidation.accept(metrics);
    }

    private void assertCause(String str, Throwable t) {
        while (t != null && !str.equals(t.getMessage())) {
            t = t.getCause();
        }
        if (t == null || !str.equals(t.getMessage())) {
            Assertions.fail("Didn't find cause: " + str);
        }
    }

    @AfterEach
    public void after() {
        gatewayServer.shutdownNow();
        executorService.shutdown();
        apiServerContainer.stop();
    }

    private long getCounter(Map<String, Metric> metrics, String name) {
        return ((Counter) metrics.get(name)).getCount();
    }

    private <T> Optional<Gauge<T>> getGauge(Map<String, Metric> metrics, String name) {
        if (!metrics.containsKey(name)) {
            return Optional.empty();
        } else {
            return Optional.of((Gauge<T>) metrics.get(name));
        }
    }

    /** Verifies that we're passing the auth headers. */
    private static class AuthServerInterceptor implements ServerInterceptor {

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> serverCall,
                Metadata metadata,
                ServerCallHandler<ReqT, RespT> serverCallHandler) {
            String authHeaderValue = metadata.get(AUTH_METADATA);
            Assertions.assertEquals("Bearer udf_def", authHeaderValue);
            return serverCallHandler.startCall(serverCall, metadata);
        }
    }
}
