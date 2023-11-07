/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.service.ForegroundResultPlan;
import io.confluent.flink.table.service.ResultPlanUtils;
import io.confluent.flink.table.service.ServiceTasks;
import io.confluent.flink.table.utils.Base64SerializationUtil;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.flink.table.service.ServiceTasks.INSTANCE;
import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_REMOTE_UDF_ENABLED;
import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_REMOTE_UDF_TARGET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT tests for remote UDF. */
@Confluent
public class RemoteUdfITCase extends AbstractTestBase {

    private static final int PARALLELISM = 4;
    private static final String SERVER_HOST_NAME = "localhost";
    private static final int SERVER_PORT = 8100;
    private static final String SERVER_TARGET = SERVER_HOST_NAME + ":" + SERVER_PORT;

    @BeforeEach
    public void before() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
    }

    static TableEnvironment getSqlServiceTableEnvironment(
            boolean udfsEnabled, boolean gatewayConfigured) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Map<String, String> confMap = new HashMap<>();
        confMap.put(CONFLUENT_REMOTE_UDF_ENABLED.key(), String.valueOf(udfsEnabled));
        confMap.put(CONFLUENT_REMOTE_UDF_TARGET.key(), gatewayConfigured ? SERVER_TARGET : "");
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), confMap, ServiceTasks.Service.SQL_SERVICE);
        return tableEnv;
    }

    static TableEnvironment getJssTableEnvironment() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // TODO: should this not be empty map?
        Map<String, String> confMap = new HashMap<>();
        confMap.put(CONFLUENT_REMOTE_UDF_TARGET.key(), SERVER_TARGET);

        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                confMap,
                ServiceTasks.Service.JOB_SUBMISSION_SERVICE);
        return tableEnv;
    }

    @Test
    public void testNumberOfBuiltinFunctions() {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(CONFLUENT_REMOTE_UDF_TARGET.key(), SERVER_TARGET);
        final RemoteUdfModule remoteUdfModule = new RemoteUdfModule(confMap);
        assertThat(remoteUdfModule.listFunctions().size()).isEqualTo(2);
        assertThat(remoteUdfModule.getFunctionDefinition("CALL_REMOTE_SCALAR")).isPresent();
        assertThat(remoteUdfModule.getFunctionDefinition("CALL_REMOTE_SCALAR_STR")).isPresent();
    }

    @Test
    public void testJssRemoteUdfsEnabled() throws Exception {
        // should be enabled by default for JSS service
        final TableEnvironment tableEnv = getJssTableEnvironment();

        final ForegroundResultPlan plan =
                ResultPlanUtils.foregroundQueryCustomConfig(
                        tableEnv,
                        "SELECT CALL_REMOTE_SCALAR('handler', 'function', 'STRING', 'payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testRemoteUdfsEnabled() throws Exception {
        // SQL service controls remote UDFs using config params
        final TableEnvironment tableEnv = getSqlServiceTableEnvironment(true, true);

        final ForegroundResultPlan plan =
                ResultPlanUtils.foregroundQueryCustomConfig(
                        tableEnv,
                        "SELECT CALL_REMOTE_SCALAR('handler', 'function', 'STRING', 'payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testRemoteUdfsDisabled() {
        final TableEnvironment tableEnv = getSqlServiceTableEnvironment(false, false);
        assertThatThrownBy(
                        () ->
                                tableEnv.executeSql(
                                        "SELECT CALL_REMOTE_SCALAR('handler', 'function', 'STRING', 'payload')"))
                .message()
                .contains("No match found for function signature CALL_REMOTE_SCALAR");
    }

    //    TODO: reactivate after demo hack is removed.
    //    @Test
    //    public void testRemoteUdfGatewayNotConfigured() {
    //        final TableEnvironment tEnv = getSqlServiceTableEnvironment(true, false);
    //        assertThatThrownBy(
    //                        () -> {
    //                            TableResult result =
    //                                    tEnv.executeSql(
    //                                            "SELECT
    // CALL_REMOTE_SCALAR(\'handler\',\'function\', \'STRING\', \'payload\');");
    //                            try (CloseableIterator<Row> iter = result.collect()) {
    //                                iter.forEachRemaining((x) -> {});
    //                            }
    //                        })
    //                .hasStackTraceContaining("Gateway target not configured");
    //    }

    @Test
    public void testRemoteUdfGateway() throws Exception {
        testRemoteUdfGatewayInternal(ArgsConcatUdfGateway.TEST_HANDLER, "funName");
        testStringRemoteUdfGatewayInternal(ArgsConcatUdfGateway.TEST_HANDLER_STR);
    }

    @Test
    public void testRemoteUdfGatewayFailOnPersistentError() {
        assertThatThrownBy(() -> testRemoteUdfGatewayInternal("NoSuchHandler", "NoFunc"))
                .hasStackTraceContaining("Unknown handler");
    }

    private void testRemoteUdfGatewayInternal(String testHandlerName, String testFunName)
            throws Exception {
        Server server = null;
        try {
            server =
                    NettyServerBuilder.forAddress(new InetSocketAddress("localhost", SERVER_PORT))
                            .addService(new ArgsConcatUdfGateway())
                            .build()
                            .start();

            final TableEnvironment tEnv = getSqlServiceTableEnvironment(true, true);
            TableResult result =
                    tEnv.executeSql(
                            String.format(
                                    "SELECT CALL_REMOTE_SCALAR('%s', '%s', 'STRING', 1, 'test', 4);",
                                    testHandlerName, testFunName));
            final List<Row> results = new ArrayList<>();
            try (CloseableIterator<Row> collect = result.collect()) {
                collect.forEachRemaining(results::add);
            }
            Assertions.assertEquals(1, results.size());
            Row row = results.get(0);
            Assertions.assertEquals("[1, test, 4]", row.getField(0));
        } finally {
            if (server != null) {
                server.shutdownNow();
            }
        }
    }

    private void testStringRemoteUdfGatewayInternal(String testFunName) throws Exception {
        String payload = "test_payload";
        Server server = null;
        try {
            server =
                    NettyServerBuilder.forAddress(new InetSocketAddress("localhost", SERVER_PORT))
                            .addService(new ArgsConcatUdfGateway())
                            .build()
                            .start();

            final TableEnvironment tEnv = getSqlServiceTableEnvironment(true, true);
            TableResult result =
                    tEnv.executeSql(
                            String.format(
                                    "SELECT CALL_REMOTE_SCALAR_STR('%s', '%s');",
                                    testFunName, payload));
            final List<Row> results = new ArrayList<>();
            try (CloseableIterator<Row> collect = result.collect()) {
                collect.forEachRemaining(results::add);
            }
            Assertions.assertEquals(1, results.size());
            Row row = results.get(0);
            Assertions.assertEquals(row.getField(0), payload);
        } finally {
            if (server != null) {
                server.shutdownNow();
            }
        }
    }

    /** Mock implementation of the UDF gateway. */
    private static class ArgsConcatUdfGateway extends UdfGatewayGrpc.UdfGatewayImplBase {

        /** The mock gateway only knows this Udf name. */
        static final String TEST_HANDLER = "test_fun";

        static final String TEST_HANDLER_STR = "test_fun_str";

        @Override
        public void invoke(
                UdfGatewayOuterClass.InvokeRequest request,
                StreamObserver<UdfGatewayOuterClass.InvokeResponse> responseObserver) {
            try {
                if (TEST_HANDLER.equals(request.getFuncName())) {
                    String payload = request.getPayload();
                    String[] payloadParts = payload.substring(0, payload.length() - 1).split(" ");
                    String responsePayload =
                            Base64SerializationUtil.serialize(
                                    (oos) ->
                                            oos.writeObject(
                                                    Base64SerializationUtil.deserialize(
                                                            payloadParts[1],
                                                            (ois) -> {
                                                                Object o = ois.readObject();
                                                                if (o.getClass().isArray()) {
                                                                    return Arrays.toString(
                                                                            (Object[]) o);
                                                                }
                                                                return String.valueOf(o);
                                                            })));

                    UdfGatewayOuterClass.InvokeResponse response =
                            UdfGatewayOuterClass.InvokeResponse.newBuilder()
                                    .setPayload('"' + responsePayload + '"')
                                    .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } else if (TEST_HANDLER_STR.equals(request.getFuncName())) {
                    {
                        UdfGatewayOuterClass.InvokeResponse response =
                                UdfGatewayOuterClass.InvokeResponse.newBuilder()
                                        .setPayload(request.getPayload())
                                        .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    }
                } else {
                    responseObserver.onError(
                            new StatusRuntimeException(
                                    Status.ABORTED.withDescription(
                                            "Unknown handler: " + request.getFuncName())));
                }
            } catch (Exception ex) {
                responseObserver.onError(ex);
            }
        }
    }
}
