/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.metrics.Counter;
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

import io.confluent.flink.apiserver.client.model.ComputeV1alphaFlinkUdfTask;
import io.confluent.flink.apiserver.client.model.ComputeV1alphaFlinkUdfTaskStatus;
import io.confluent.flink.table.modules.remoteudf.mock.MockedFunctionWithTypes;
import io.confluent.flink.table.modules.remoteudf.mock.MockedUdfGateway;
import io.confluent.flink.table.modules.remoteudf.testcontainers.ApiServerContainer;
import io.confluent.flink.table.modules.remoteudf.util.ApiServerUtils;
import io.confluent.flink.table.modules.remoteudf.util.TestUtils;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetSocketAddress;
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

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.BYES_FROM_UDF_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.BYES_TO_UDF_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.DEPROVISIONS_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.INVOCATION_FAILURES_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.INVOCATION_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.INVOCATION_SUCCESSES_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.METRIC_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfMetrics.PROVISIONS_NAME;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID;
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

    private static final String TEST_ORG = "test-org";
    private static final String TEST_ENV = "test-env";
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
                Collection<ComputeV1alphaFlinkUdfTask> udfTasks =
                        ApiServerUtils.listPendingUdfTasks(apiServerContainer, TEST_ORG, TEST_ENV);
                if (udfTasks.size() > 0) {
                    ComputeV1alphaFlinkUdfTask udfTask = udfTasks.iterator().next();
                    // Make sure metadata from Payload is properly propagated
                    RemoteUdfSpec udfSpec =
                            RemoteUdfSpec.deserialize(
                                    new DataInputDeserializer(
                                            udfTask.getSpec().getEntryPoint().getOpenPayload()),
                                    Thread.currentThread().getContextClassLoader());
                    testUdfGateway.registerUdfSpec(udfSpec);

                    // Set to Running to move to GW invocation
                    udfTask.getStatus()
                            .setPhase(ComputeV1alphaFlinkUdfTaskStatus.PhaseEnum.RUNNING);
                    udfTask.getStatus().getEndpoint().setHost(GW_HOST);
                    udfTask.getStatus().getEndpoint().setPort(GW_PORT);
                    apiServerContainer
                            .getComputeV1alphaApi()
                            .updateComputeV1alphaFlinkUdfTaskStatus(
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
        gatewayServer =
                NettyServerBuilder.forAddress(new InetSocketAddress(GW_HOST, GW_PORT))
                        .addService(testUdfGateway)
                        .build()
                        .start();

        apiServerContainer = new ApiServerContainer();
        apiServerContainer.start();

        ApiServerUtils.createTestEnvAndOrg(apiServerContainer, TEST_ORG, TEST_ENV);
        // Periodic service that marks UdfTask ready with metadata
        executorService = Executors.newSingleThreadScheduledExecutor();
        // Shutdown on completion
        executorService.scheduleAtFixedRate(
                new UdfTaskCallable(apiServerContainer, testUdfGateway),
                100,
                100,
                TimeUnit.MILLISECONDS);
    }

    private void testRemoteUdfGatewayInternal(boolean jss) throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(
                CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER.key(),
                apiServerContainer.getHostAddress());
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID.key(), "cpp-udf-shim");
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID.key(), "ver-udf-shim-1");
        final TableEnvironment tEnv =
                jss
                        ? TestUtils.getJssTableEnvironment(confMap, testFunctionMeta)
                        : TestUtils.getSqlServiceTableEnvironment(
                                confMap, testFunctionMeta, true, false);
        TableResult result = tEnv.executeSql("SELECT cat1.db1.remote1(1, 'test', 4);");
        JobID jobID = result.getJobClient().get().getJobID();
        final List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> collect = result.collect()) {
            collect.forEachRemaining(results::add);
        }
        Assertions.assertEquals(1, results.size());
        Row row = results.get(0);
        Assertions.assertEquals("str:[1, test, 4]", row.getField(0));

        Collection<ComputeV1alphaFlinkUdfTask> udfTasks =
                ApiServerUtils.listRunningUdfTasks(apiServerContainer, TEST_ORG, TEST_ENV);
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
        Assertions.assertEquals(1, getCounter(metrics, INVOCATION_NAME));
        Assertions.assertEquals(1, getCounter(metrics, INVOCATION_SUCCESSES_NAME));
        Assertions.assertEquals(0, getCounter(metrics, INVOCATION_FAILURES_NAME));
        Assertions.assertEquals(1, getCounter(metrics, PROVISIONS_NAME));
        Assertions.assertEquals(1, getCounter(metrics, DEPROVISIONS_NAME));
        Assertions.assertEquals(16, getCounter(metrics, BYES_TO_UDF_NAME));
        Assertions.assertEquals(20, getCounter(metrics, BYES_FROM_UDF_NAME));
    }

    @Test
    public void testRemoteUdfGateway() throws Exception {
        testRemoteUdfGatewayInternal(false);
    }

    @Test
    public void testRemoteUdfGateway_jss() throws Exception {
        testRemoteUdfGatewayInternal(true);
    }

    @Test
    public void testRemoteUdfApiServerNotConfigured() {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID.key(), "cpp-udf-shim");
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID.key(), "ver-udf-shim-1");
        final TableEnvironment tableEnv =
                TestUtils.getSqlServiceTableEnvironment(confMap, testFunctionMeta, true, false);
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
    public void testRemoteUdfGateway_error() throws Exception {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(
                CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER.key(),
                apiServerContainer.getHostAddress());
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID.key(), "cpp-udf-shim");
        confMap.put(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID.key(), "ver-udf-shim-1");
        final TableEnvironment tEnv =
                TestUtils.getSqlServiceTableEnvironment(confMap, testFunctionMeta, true, false);
        TableResult result = tEnv.executeSql("SELECT cat1.db1.error(1, 'test', 4);");
        JobID jobID = result.getJobClient().get().getJobID();
        Assertions.assertThrows(ExecutionException.class, () -> result.await());
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
        Assertions.assertEquals(1, getCounter(metrics, INVOCATION_NAME));
        Assertions.assertEquals(0, getCounter(metrics, INVOCATION_SUCCESSES_NAME));
        Assertions.assertEquals(1, getCounter(metrics, INVOCATION_FAILURES_NAME));
        Assertions.assertEquals(1, getCounter(metrics, PROVISIONS_NAME));
        Assertions.assertEquals(1, getCounter(metrics, DEPROVISIONS_NAME));
        Assertions.assertEquals(16, getCounter(metrics, BYES_TO_UDF_NAME));
        Assertions.assertEquals(0, getCounter(metrics, BYES_FROM_UDF_NAME));
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
}
