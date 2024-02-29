/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.util;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.DatabaseInfo;
import io.confluent.flink.table.modules.remoteudf.mock.MockedCatalog;
import io.confluent.flink.table.modules.remoteudf.mock.MockedFunctionWithTypes;
import io.confluent.flink.table.service.ServiceTasks;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTIONS_PREFIX;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ARGUMENT_TYPES_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CATALOG_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CLASS_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_DATABASE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ENV_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ORG_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_RETURN_TYPE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.PLUGIN_ID_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.PLUGIN_VERSION_ID_FIELD;
import static io.confluent.flink.table.service.ServiceTasks.INSTANCE;
import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_REMOTE_UDF_ENABLED;

/** Test utility class with common functionality for Remote UDF testing. */
public class TestUtils {
    static final String GW_SERVER_TARGET = "localhost:50051";
    static final String APISERVER_TARGET = "http://localhost:8080";
    public static final int EXPECTED_INT_RETURN_VALUE = 424242;

    public static Map<String, String> getBaseConfigMap() {
        return getBaseConfigMap(GW_SERVER_TARGET, APISERVER_TARGET);
    }

    public static Map<String, String> getBaseConfigMap(
            String gatewayTarget, String apiserverTarget) {
        Map<String, String> confMap = new HashMap<>();
        confMap.put(CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER.key(), apiserverTarget);
        return confMap;
    }

    public static TableEnvironment getSqlServiceTableEnvironment(
            MockedFunctionWithTypes[] testFunctions, boolean udfsEnabled, boolean createCatalog) {
        return getSqlServiceTableEnvironment(
                getBaseConfigMap(), testFunctions, udfsEnabled, createCatalog);
    }

    public static TableEnvironment getSqlServiceTableEnvironment(
            Map<String, String> confMap,
            MockedFunctionWithTypes[] testFunctions,
            boolean udfsEnabled,
            boolean createCatalog) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        if (createCatalog) {
            createCatalog(tableEnv);
        }
        if (udfsEnabled) {
            TestUtils.populateServiceTaskConfFromMockedFunctions(confMap, testFunctions);
            confMap.put(CONFLUENT_REMOTE_UDF_ENABLED.key(), String.valueOf(true));
        }
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), confMap, ServiceTasks.Service.SQL_SERVICE);
        return tableEnv;
    }

    public static TableEnvironment getJssTableEnvironment(MockedFunctionWithTypes[] testFunctions) {
        return getJssTableEnvironment(getBaseConfigMap(), testFunctions);
    }

    public static TableEnvironment getJssTableEnvironment(
            Map<String, String> confMap, MockedFunctionWithTypes[] testFunctions) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        TestUtils.populateServiceTaskConfFromMockedFunctions(confMap, testFunctions);
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
                new MockedCatalog(
                        CatalogInfo.of("env-1", "cat1"),
                        Collections.singletonList(DatabaseInfo.of("lkc-1", "db1"))));
    }

    /**
     * Populates UDF conf as used by the ServiceTasks module to load
     * ConfiguredRemoteScalarFunctions.
     */
    public static void populateServiceTaskConfFromMockedFunctions(
            Map<String, String> udfConf, MockedFunctionWithTypes[] funcs) {
        for (MockedFunctionWithTypes func : funcs) {
            Preconditions.checkState(func.getArgTypes().size() == func.getReturnType().size());
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_ORG_FIELD, "test-org");
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_ENV_FIELD, "test-env");
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_CATALOG_FIELD, "cat1");
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_DATABASE_FIELD, "db1");
            udfConf.put(
                    FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_NAME_FIELD, func.getName());
            udfConf.put(FUNCTIONS_PREFIX + func.getName() + "." + PLUGIN_ID_FIELD, "test-pluginId");
            udfConf.put(
                    FUNCTIONS_PREFIX + func.getName() + "." + PLUGIN_VERSION_ID_FIELD,
                    "test-versionId");
            udfConf.put(
                    FUNCTIONS_PREFIX + func.getName() + "." + FUNCTION_CLASS_NAME_FIELD,
                    "io.confluent.ExampleFunction");
            for (int i = 0; i < func.getArgTypes().size(); i++) {
                udfConf.put(
                        FUNCTIONS_PREFIX
                                + func.getName()
                                + "."
                                + FUNCTION_ARGUMENT_TYPES_FIELD
                                + "."
                                + i,
                        String.join(";", func.getArgTypes().get(i)));
                udfConf.put(
                        FUNCTIONS_PREFIX
                                + func.getName()
                                + "."
                                + FUNCTION_RETURN_TYPE_FIELD
                                + "."
                                + i,
                        func.getReturnType().get(i));
            }
        }
    }
}
