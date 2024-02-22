/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.catalog.DatabaseInfo;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.infoschema.InfoSchemaTables;
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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_TARGET;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTIONS_PREFIX;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ARGUMENT_TYPES_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CATALOG_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CLASS_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_DATABASE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_RETURN_TYPE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.PLUGIN_ID_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.PLUGIN_VERSION_ID_FIELD;
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
    private static final int INT_RETURN_VALUE = 424242;

    @BeforeEach
    public void before() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
    }

    static TableEnvironment getSqlServiceTableEnvironment(
            boolean udfsEnabled,
            boolean gatewayConfigured,
            boolean createCatalog,
            String functionId) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        Map<String, String> confMap = new HashMap<>();
        confMap.put(CONFLUENT_REMOTE_UDF_ENABLED.key(), String.valueOf(udfsEnabled));
        confMap.put(CONFLUENT_REMOTE_UDF_TARGET.key(), gatewayConfigured ? SERVER_TARGET : "");
        if (createCatalog) {
            createCatalog(tableEnv);
        }
        if (udfsEnabled) {
            registerUdf(
                    confMap,
                    new TestFunc[] {
                        new TestFunc(
                                "remote1",
                                ImmutableList.of(
                                        new String[] {"INT", "STRING", "INT"},
                                        new String[] {"INT"}),
                                ImmutableList.of("STRING", "INT"),
                                functionId),
                        new TestFunc("remote2", new String[] {"STRING"}, "STRING", functionId),
                        new TestFunc("remote3", new String[] {}, "STRING", functionId)
                    });
        }
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

        registerUdf(
                confMap,
                new TestFunc[] {
                    new TestFunc(
                            "remote1",
                            ImmutableList.of(
                                    new String[] {"INT", "STRING", "INT"}, new String[] {"INT"}),
                            ImmutableList.of("STRING", "INT")),
                    new TestFunc("remote2", new String[] {"STRING"}, "STRING"),
                    new TestFunc("remote3", new String[] {}, "STRING")
                });
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                confMap,
                ServiceTasks.Service.JOB_SUBMISSION_SERVICE);
        return tableEnv;
    }

    private static void createCatalog(final TableEnvironment tableEnv) {
        tableEnv.registerCatalog(
                "cat1",
                new TestCatalog(
                        CatalogInfo.of("env-1", "cat1"),
                        Collections.singletonList(DatabaseInfo.of("lkc-1", "db1"))));
    }

    @Test
    public void testNumberOfBuiltinFunctions() {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(CONFLUENT_REMOTE_UDF_TARGET.key(), SERVER_TARGET);
        registerUdf(
                confMap,
                new TestFunc[] {
                    new TestFunc("remote1", new String[] {"INT", "STRING", "INT"}, "STRING"),
                    new TestFunc("remote2", new String[] {"STRING"}, "STRING")
                });
        List<ConfiguredRemoteScalarFunction> functions = UdfUtil.extractUdfs(confMap);
        final RemoteUdfModule remoteUdfModule = new RemoteUdfModule(functions);
        assertThat(remoteUdfModule.listFunctions().size()).isEqualTo(2);
        assertThat(remoteUdfModule.getFunctionDefinition("SYSTEM_CAT1_DB1_REMOTE1")).isPresent();
        assertThat(remoteUdfModule.getFunctionDefinition("SYSTEM_CAT1_DB1_REMOTE2")).isPresent();
    }

    @Test
    public void testJssRemoteUdfsEnabled() throws Exception {
        // should be enabled by default for JSS service
        final TableEnvironment tableEnv = getJssTableEnvironment();

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJobCustomConfig(
                        tableEnv, "SELECT cat1.db1.remote2('payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testJssRemoteUdfsNoExpressionReducer() throws Exception {
        // should be enabled by default for JSS service
        final TableEnvironment tableEnv = getJssTableEnvironment();

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJobCustomConfig(tableEnv, "SELECT cat1.db1.remote1(1)");
        // The value of remote(1) is INT_RETURN_VALUE, but we want to ensure that the
        // Expression reducer didn't inline the value into the plan.
        assertThat(plan.getCompiledPlan()).doesNotContain(Integer.toString(INT_RETURN_VALUE));
    }

    @Test
    public void testRemoteUdfsEnabled() throws Exception {
        // SQL service controls remote UDFs using config params
        final TableEnvironment tableEnv =
                getSqlServiceTableEnvironment(
                        true, true, false, TestUdfGateway.IDENTITY_FUNCTION_ID);

        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJobCustomConfig(
                        tableEnv, "SELECT cat1.db1.remote2('payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testRemoteUdfsEnabled_useCatalogDb() throws Exception {
        // SQL service controls remote UDFs using config params
        final TableEnvironment tableEnv =
                getSqlServiceTableEnvironment(
                        true, true, true, TestUdfGateway.IDENTITY_FUNCTION_ID);
        tableEnv.executeSql("USE CATALOG cat1");
        tableEnv.executeSql("USE db1");
        final ForegroundJobResultPlan plan =
                ResultPlanUtils.foregroundJobCustomConfig(tableEnv, "SELECT remote2('payload')");
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    public void testRemoteUdfsDisabled() {
        final TableEnvironment tableEnv =
                getSqlServiceTableEnvironment(
                        false, false, false, TestUdfGateway.IDENTITY_FUNCTION_ID);
        assertThatThrownBy(() -> tableEnv.executeSql("SELECT remote2('payload')"))
                .message()
                .contains("No match found for function signature remote2");
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
        testRemoteUdfGatewayInternal(TestUdfGateway.IDENTITY_FUNCTION_ID, false);
    }

    @Test
    public void testRemoteUdfGatewayFailOnPersistentError() {
        assertThatThrownBy(() -> testRemoteUdfGatewayInternal("NoSuchHandler", false))
                .hasStackTraceContaining("errorCode 1: Unknown function id: NoSuchHandler");
    }

    @Test
    public void testRemoteUdfGateway_jss() throws Exception {
        testRemoteUdfGatewayInternal(TestUdfGateway.IDENTITY_FUNCTION_ID, true);
    }

    private static void registerUdf(Map<String, String> udfConf, TestFunc[] funcs) {
        for (TestFunc func : funcs) {
            Preconditions.checkState(func.argTypes.size() == func.returnType.size());
            udfConf.put(FUNCTIONS_PREFIX + func.name + "." + FUNCTION_CATALOG_FIELD, "cat1");
            udfConf.put(FUNCTIONS_PREFIX + func.name + "." + FUNCTION_DATABASE_FIELD, "db1");
            udfConf.put(FUNCTIONS_PREFIX + func.name + "." + FUNCTION_NAME_FIELD, func.name);
            udfConf.put(FUNCTIONS_PREFIX + func.name + "." + PLUGIN_ID_FIELD, func.functionId);
            udfConf.put(
                    FUNCTIONS_PREFIX + func.name + "." + PLUGIN_VERSION_ID_FIELD, func.functionId);
            udfConf.put(
                    FUNCTIONS_PREFIX + func.name + "." + FUNCTION_CLASS_NAME_FIELD,
                    "io.confluent.blah1");
            for (int i = 0; i < func.argTypes.size(); i++) {
                udfConf.put(
                        FUNCTIONS_PREFIX
                                + func.name
                                + "."
                                + FUNCTION_ARGUMENT_TYPES_FIELD
                                + "."
                                + i,
                        String.join(";", func.argTypes.get(i)));
                udfConf.put(
                        FUNCTIONS_PREFIX + func.name + "." + FUNCTION_RETURN_TYPE_FIELD + "." + i,
                        func.returnType.get(i));
            }
        }
    }

    private void testRemoteUdfGatewayInternal(String testHandlerName, boolean jss)
            throws Exception {
        Server server = null;
        try {
            TestUdfGateway testUdfGateway = new TestUdfGateway();
            server =
                    NettyServerBuilder.forAddress(new InetSocketAddress("localhost", SERVER_PORT))
                            .addService(testUdfGateway)
                            .build()
                            .start();

            final TableEnvironment tEnv =
                    jss
                            ? getJssTableEnvironment()
                            : getSqlServiceTableEnvironment(true, true, false, testHandlerName);
            TableResult result = tEnv.executeSql("SELECT cat1.db1.remote1(1, 'test', 4);");
            final List<Row> results = new ArrayList<>();
            try (CloseableIterator<Row> collect = result.collect()) {
                collect.forEachRemaining(results::add);
            }
            Assertions.assertEquals(1, results.size());
            Row row = results.get(0);
            Assertions.assertEquals("[1, test, 4]", row.getField(0));
            Assertions.assertTrue(testUdfGateway.instanceToFuncIds.isEmpty());

            TableResult result2 = tEnv.executeSql("SELECT cat1.db1.remote1(1);");
            final List<Row> results2 = new ArrayList<>();
            try (CloseableIterator<Row> collect = result2.collect()) {
                collect.forEachRemaining(results2::add);
            }
            Assertions.assertEquals(1, results2.size());
            Row row2 = results2.get(0);
            Assertions.assertEquals(INT_RETURN_VALUE, row2.getField(0));
            Assertions.assertTrue(testUdfGateway.instanceToFuncIds.isEmpty());

            TableResult result3 = tEnv.executeSql("SELECT cat1.db1.remote3();");
            final List<Row> results3 = new ArrayList<>();
            try (CloseableIterator<Row> collect = result3.collect()) {
                collect.forEachRemaining(results3::add);
            }
            Assertions.assertEquals(1, results3.size());
            Row row3 = results3.get(0);
            Assertions.assertEquals("[]", row3.getField(0));
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
        static final String IDENTITY_FUNCTION_ID = "IdentityFunction";
        static final String IDENTITY_FUNCTION2_ID = "IdentityFunction2";
        final Map<String, RemoteUdfSpec> instanceToFuncIds = new HashMap<>();

        // TODO: FRT-321 Replace createInstance and deleteInstance with UDFTask calls to apiServer
        @Override
        public void createInstance(
                CreateInstanceRequest request,
                StreamObserver<CreateInstanceResponse> responseObserver) {

            try {
                CreateInstanceResponse.Builder builder = CreateInstanceResponse.newBuilder();

                if (IDENTITY_FUNCTION_ID.equals(request.getMetadata().getName())) {
                    DataInputDeserializer in =
                            new DataInputDeserializer(
                                    request.getSpec()
                                            .getEntryPoint()
                                            .getOpenPayload()
                                            .asReadOnlyByteBuffer());

                    RemoteUdfSpec udfSpec =
                            RemoteUdfSpec.deserialize(
                                    in, Thread.currentThread().getContextClassLoader());

                    instanceToFuncIds.put(IDENTITY_FUNCTION_ID, udfSpec);
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
                    if (remoteUdfSpec
                            .getReturnType()
                            .getLogicalType()
                            .is(DataTypes.STRING().getLogicalType().getTypeRoot())) {
                        builder.setPayload(
                                serialization.serializeReturnValue(
                                        BinaryStringData.fromString(
                                                Arrays.asList(args).toString())));
                    } else if (remoteUdfSpec
                            .getReturnType()
                            .getLogicalType()
                            .is(DataTypes.INT().getLogicalType().getTypeRoot())) {
                        builder.setPayload(serialization.serializeReturnValue(INT_RETURN_VALUE));
                    } else {
                        throw new Exception(
                                "Unknown return type "
                                        + remoteUdfSpec.getReturnType().getLogicalType());
                    }
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

    /** Test utility class. */
    public static class TestFunc {
        final String name;
        final List<String[]> argTypes;
        final List<String> returnType;
        final String functionId;

        public TestFunc(String name, List<String[]> argTypes, List<String> returnType) {
            this(name, argTypes, returnType, TestUdfGateway.IDENTITY_FUNCTION_ID);
        }

        public TestFunc(String name, String[] argTypes, String returnType) {
            this(
                    name,
                    Collections.singletonList(argTypes),
                    Collections.singletonList(returnType),
                    TestUdfGateway.IDENTITY_FUNCTION_ID);
        }

        public TestFunc(String name, String[] argTypes, String returnType, String functionId) {
            this(
                    name,
                    Collections.singletonList(argTypes),
                    Collections.singletonList(returnType),
                    functionId);
        }

        public TestFunc(
                String name, List<String[]> argTypes, List<String> returnType, String functionId) {
            this.name = name;
            this.argTypes = argTypes;
            this.returnType = returnType;
            this.functionId = functionId;
        }
    }

    /** A test catalog for registering the UDFs. */
    public static class TestCatalog extends GenericInMemoryCatalog implements Catalog {

        private final CatalogInfo catalogInfo;
        private final List<DatabaseInfo> databaseInfos;

        public TestCatalog(CatalogInfo catalogInfo, List<DatabaseInfo> databaseInfos) {
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
                    null,
                    table.getPartitionKeys(),
                    table.getOptions(),
                    Collections.emptyMap());
        }

        public List<DatabaseInfo> listDatabaseInfos() {
            // Add INFORMATION_SCHEMA to databases
            return Stream.concat(
                            databaseInfos.stream(),
                            Stream.of(InfoSchemaTables.INFORMATION_SCHEMA_DATABASE_INFO))
                    .collect(Collectors.toList());
        }

        public CatalogInfo getCatalogInfo() {
            return catalogInfo;
        }
    }
}
