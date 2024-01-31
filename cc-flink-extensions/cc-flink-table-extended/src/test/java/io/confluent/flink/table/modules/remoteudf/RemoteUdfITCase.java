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
import io.confluent.secure.compute.gateway.v1.CreateInstanceRequest;
import io.confluent.secure.compute.gateway.v1.CreateInstanceResponse;
import io.confluent.secure.compute.gateway.v1.DeleteInstanceRequest;
import io.confluent.secure.compute.gateway.v1.DeleteInstanceResponse;
import io.confluent.secure.compute.gateway.v1.Error;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionRequest;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionResponse;
import io.confluent.secure.compute.gateway.v1.SecureComputeGatewayGrpc;
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

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_TARGET;
import static io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import static io.confluent.flink.table.service.ServiceTasks.INSTANCE;
import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_REMOTE_UDF_ENABLED;
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
    private static class TestUdfGateway
            extends SecureComputeGatewayGrpc.SecureComputeGatewayImplBase {
        static final String IDENTIFY_FUNCTION_ID = "IdentityFunction";
        final Map<String, RemoteUdfSpec> instanceToFuncIds = new HashMap<>();

        // TODO: FRT-321 Replace createInstance and deleteInstance with UDFTask calls to apiServer
        @Override
        public void createInstance(
                CreateInstanceRequest request,
                StreamObserver<CreateInstanceResponse> responseObserver) {

            try {
                CreateInstanceResponse.Builder builder = CreateInstanceResponse.newBuilder();

                if (IDENTIFY_FUNCTION_ID.equals(request.getMetadata().getName())) {
                    DataInputDeserializer in =
                            new DataInputDeserializer(
                                    request.getSpec()
                                            .getEntryPoint()
                                            .getOpenPayload()
                                            .asReadOnlyByteBuffer());

                    RemoteUdfSpec udfSpec =
                            RemoteUdfSpec.deserialize(
                                    in, Thread.currentThread().getContextClassLoader());

                    instanceToFuncIds.put(IDENTIFY_FUNCTION_ID, udfSpec);
                } else {
                    builder.setError(
                            Error.newBuilder()
                                    .setCode(1)
                                    .setMessage(
                                            "Unknown function id: "
                                                    + request.getMetadata().getName())
                                    .build());
                }
                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            } catch (Exception ex) {
                responseObserver.onError(ex);
            }
        }

        @Override
        public void deleteInstance(
                DeleteInstanceRequest request,
                StreamObserver<DeleteInstanceResponse> responseObserver) {

            DeleteInstanceResponse.Builder builder = DeleteInstanceResponse.newBuilder();
            String functionNameToDelete = request.getMetadata().getName();
            if (instanceToFuncIds.remove(functionNameToDelete) == null) {
                builder.setError(
                        Error.newBuilder()
                                .setCode(1)
                                .setMessage("Function not found: " + functionNameToDelete)
                                .build());
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void invokeFunction(
                InvokeFunctionRequest request,
                StreamObserver<InvokeFunctionResponse> responseObserver) {

            InvokeFunctionResponse.Builder builder = InvokeFunctionResponse.newBuilder();

            try {
                RemoteUdfSpec remoteUdfSpec = instanceToFuncIds.get(request.getFuncInstanceName());

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
                    throw new Exception(
                            "Unknown Function instance: " + request.getFuncInstanceName());
                }
            } catch (Exception ex) {
                builder.setError(Error.newBuilder().setCode(1).setMessage(ex.getMessage()).build());
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }
}
