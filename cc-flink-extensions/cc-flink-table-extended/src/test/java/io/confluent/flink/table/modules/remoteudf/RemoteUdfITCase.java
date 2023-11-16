/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.service.ResultPlanUtils;
import io.confluent.flink.table.service.ServiceTasks;
import io.grpc.Server;
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
import java.util.UUID;

import static io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
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
        assertThat(remoteUdfModule.listFunctions().size()).isEqualTo(1);
        assertThat(remoteUdfModule.getFunctionDefinition("CALL_REMOTE_SCALAR")).isPresent();
    }

    @Test
    public void testJssRemoteUdfsEnabled() throws Exception {
        // should be enabled by default for JSS service
        final TableEnvironment tableEnv = getJssTableEnvironment();

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJobCustomConfig(
                        tableEnv,
                        "SELECT CALL_REMOTE_SCALAR('handler', 'function', 'STRING', 'payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testRemoteUdfsEnabled() throws Exception {
        // SQL service controls remote UDFs using config params
        final TableEnvironment tableEnv = getSqlServiceTableEnvironment(true, true);

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJobCustomConfig(
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
        testRemoteUdfGatewayInternal(TestUdfGateway.IDENTIFY_FUNCTION_ID, "funName");
    }

    @Test
    public void testRemoteUdfGatewayFailOnPersistentError() {
        assertThatThrownBy(() -> testRemoteUdfGatewayInternal("NoSuchHandler", "NoFunc"))
                .hasStackTraceContaining("errorCode 1: Unknown function id: NoSuchHandler");
    }

    private void testRemoteUdfGatewayInternal(String testHandlerName, String testFunName)
            throws Exception {
        Server server = null;
        try {
            TestUdfGateway testUdfGateway = new TestUdfGateway();
            server =
                    NettyServerBuilder.forAddress(new InetSocketAddress("localhost", SERVER_PORT))
                            .addService(testUdfGateway)
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
            Assertions.assertTrue(testUdfGateway.instanceToFuncIds.isEmpty());
        } finally {
            if (server != null) {
                server.shutdownNow();
            }
        }
    }

    /** Mock implementation of the UDF gateway. */
    private static class TestUdfGateway extends UdfGatewayGrpc.UdfGatewayImplBase {

        static final String IDENTIFY_FUNCTION_ID = "IdentityFunction";
        final Map<String, RemoteUdfSpec> instanceToFuncIds = new HashMap<>();

        @Override
        public void invoke(
                UdfGatewayOuterClass.InvokeRequest request,
                StreamObserver<UdfGatewayOuterClass.InvokeResponse> responseObserver) {

            UdfGatewayOuterClass.InvokeResponse.Builder builder =
                    UdfGatewayOuterClass.InvokeResponse.newBuilder();

            try {
                RemoteUdfSpec remoteUdfSpec = instanceToFuncIds.get(request.getFuncInstanceId());

                if (remoteUdfSpec != null) {
                    List<TypeSerializer<Object>> argumentSerializers =
                            remoteUdfSpec.createArgumentSerializers();
                    RemoteUdfSerialization serialization =
                            new RemoteUdfSerialization(
                                    remoteUdfSpec.createReturnTypeSerializer(),
                                    argumentSerializers);
                    Object[] args = new Object[argumentSerializers.size()];
                    serialization.deserializeArguments(
                            request.getPayload().asReadOnlyByteBuffer(), args);
                    builder.setPayload(
                            serialization.serializeReturnValue(
                                    BinaryStringData.fromString(Arrays.asList(args).toString())));
                } else {
                    throw new Exception("Unknown instance: " + request.getFuncInstanceId());
                }
            } catch (Exception ex) {
                builder.setError(
                        UdfGatewayOuterClass.Error.newBuilder()
                                .setCode(1)
                                .setMessage(ex.getMessage())
                                .build());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void createInstances(
                UdfGatewayOuterClass.CreateInstancesRequest request,
                StreamObserver<UdfGatewayOuterClass.CreateInstancesResponse> responseObserver) {

            try {
                UdfGatewayOuterClass.CreateInstancesResponse.Builder builder =
                        UdfGatewayOuterClass.CreateInstancesResponse.newBuilder();

                if (IDENTIFY_FUNCTION_ID.equals(request.getFuncId())) {

                    DataInputDeserializer in =
                            new DataInputDeserializer(request.getPayload().asReadOnlyByteBuffer());

                    RemoteUdfSpec udfSpec =
                            RemoteUdfSpec.deserialize(
                                    in, Thread.currentThread().getContextClassLoader());

                    List<String> newInstanceIds = new ArrayList<>(request.getNumInstances());
                    for (int i = 0; i < request.getNumInstances(); ++i) {
                        String uuid = UUID.randomUUID().toString();
                        newInstanceIds.add(uuid);
                        instanceToFuncIds.put(uuid, udfSpec);
                    }
                    builder.addAllFuncInstanceIds(newInstanceIds);
                } else {
                    builder.setError(
                            UdfGatewayOuterClass.Error.newBuilder()
                                    .setCode(1)
                                    .setMessage("Unknown function id: " + request.getFuncId())
                                    .build());
                }

                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            } catch (Exception ex) {
                responseObserver.onError(ex);
            }
        }

        @Override
        public void deleteInstances(
                UdfGatewayOuterClass.DeleteInstancesRequest request,
                StreamObserver<UdfGatewayOuterClass.DeleteInstancesResponse> responseObserver) {

            List<String> error = new ArrayList<>(0);
            for (String instanceIdToDelete : request.getFuncInstanceIdsList()) {
                if (instanceToFuncIds.remove(instanceIdToDelete) == null) {
                    error.add(instanceIdToDelete);
                }
            }

            UdfGatewayOuterClass.DeleteInstancesResponse.Builder builder =
                    UdfGatewayOuterClass.DeleteInstancesResponse.newBuilder();
            if (!error.isEmpty()) {
                builder.setError(
                        UdfGatewayOuterClass.Error.newBuilder()
                                .setCode(1)
                                .setMessage(error.toString())
                                .build());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }
}
