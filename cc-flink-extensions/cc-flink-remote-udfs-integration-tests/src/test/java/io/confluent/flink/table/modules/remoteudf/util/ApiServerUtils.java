/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.util;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.apiserver.client.ApiException;
import io.confluent.flink.apiserver.client.CoreV1Api;
import io.confluent.flink.apiserver.client.model.ApisMetaV1ObjectMeta;
import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTask;
import io.confluent.flink.apiserver.client.model.CoreV1Environment;
import io.confluent.flink.apiserver.client.model.CoreV1EnvironmentSpec;
import io.confluent.flink.apiserver.client.model.CoreV1Org;
import io.confluent.flink.table.modules.remoteudf.testcontainers.ApiServerContainer;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Collectors;

/** Common utilities for the ApiServer. */
public class ApiServerUtils {

    public static void createEnv(CoreV1Api coreApi, final String orgName, final String envName)
            throws ApiException {
        CoreV1EnvironmentSpec envSpec = new CoreV1EnvironmentSpec();
        envSpec.setDisplayName(envName);
        CoreV1Environment env =
                new CoreV1Environment()
                        .metadata(new ApisMetaV1ObjectMeta().org(orgName).name(envName));
        env.setSpec(envSpec);
        coreApi.createCoreV1Environment(orgName, env);
    }

    public static void createTestEnvAndOrg(
            ApiServerContainer container, String testOrg, String testEnv) throws Exception {
        CoreV1Api coreV1Api = new CoreV1Api(container.getClient());

        coreV1Api.createCoreV1Org(
                new CoreV1Org()
                        .metadata(
                                new ApisMetaV1ObjectMeta()
                                        .name(testOrg)
                                        .labels(ImmutableMap.of("resource_id", testOrg))));

        createEnv(coreV1Api, testOrg, testEnv);
        createEnv(coreV1Api, testOrg, "env2");
    }

    public static Collection<String> listEnvironmentsIds(ApiServerContainer container, String org)
            throws Exception {
        try {
            return container.getCoreV1Api()
                    .listCoreV1Environments(org, null, false, null, null, null).getItems().stream()
                    .map(env -> env.getMetadata().getName())
                    .collect(Collectors.toSet());
        } catch (ApiException e) {
            throw new Exception(e.getResponseBody(), e);
        }
    }

    public static Collection<ComputeV1FlinkUdfTask> listPendingUdfTasks(
            ApiServerContainer container, String org, String env) throws Exception {
        try {
            return new HashSet<>(
                    container
                            .getComputeV1Api()
                            .listComputeV1FlinkUdfTasks(
                                    env, org, null, null, "status.phase=Pending", null, 1)
                            .getItems());
        } catch (ApiException e) {
            throw new Exception(e.getResponseBody(), e);
        }
    }

    public static Collection<ComputeV1FlinkUdfTask> listRunningUdfTasks(
            ApiServerContainer container, String org, String env) throws Exception {
        try {
            return new HashSet<>(
                    container
                            .getComputeV1Api()
                            .listComputeV1FlinkUdfTasks(
                                    env, org, null, null, "status.phase=Running", null, 1)
                            .getItems());
        } catch (ApiException e) {
            throw new Exception(e.getResponseBody(), e);
        }
    }
}
