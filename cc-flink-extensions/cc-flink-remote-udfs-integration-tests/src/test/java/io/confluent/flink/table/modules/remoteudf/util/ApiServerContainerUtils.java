/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.util;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.apiserver.client.ApiException;
import io.confluent.flink.apiserver.client.CoreV1Api;
import io.confluent.flink.apiserver.client.FlinkV1Api;
import io.confluent.flink.apiserver.client.model.ApisMetaV1EnvironmentLocalObjectReference;
import io.confluent.flink.apiserver.client.model.ApisMetaV1ObjectMeta;
import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTask;
import io.confluent.flink.apiserver.client.model.CoreV1Environment;
import io.confluent.flink.apiserver.client.model.CoreV1EnvironmentSpec;
import io.confluent.flink.apiserver.client.model.CoreV1Org;
import io.confluent.flink.apiserver.client.model.FlinkV1ComputePool;
import io.confluent.flink.apiserver.client.model.FlinkV1ComputePoolResources;
import io.confluent.flink.apiserver.client.model.FlinkV1ComputePoolSpec;
import io.confluent.flink.apiserver.client.model.FlinkV1Job;
import io.confluent.flink.apiserver.client.model.FlinkV1JobSpec;
import io.confluent.flink.apiserver.client.model.FlinkV1RuntimeRef;
import io.confluent.flink.table.modules.remoteudf.testcontainers.ApiServerContainer;

import java.util.Collection;
import java.util.HashSet;

/** Common utilities for the ApiServer. */
public class ApiServerContainerUtils {

    /** Create an environment with the given name in the given org. */
    private static void createEnv(CoreV1Api coreApi, final String orgName, final String envName)
            throws ApiException {
        CoreV1EnvironmentSpec envSpec = new CoreV1EnvironmentSpec();
        envSpec.setDisplayName(envName);
        CoreV1Environment env =
                new CoreV1Environment()
                        .metadata(new ApisMetaV1ObjectMeta().org(orgName).name(envName));
        env.setSpec(envSpec);
        coreApi.createCoreV1Environment(orgName, env);
    }

    /** Create a test environment and org. */
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

    /** Create a test compute pool with a fake runtime. */
    public static void createComputePool(
            ApiServerContainer container, String cell, String org, String env, String computePool)
            throws ApiException {
        FlinkV1Api flinkV1Api = new FlinkV1Api(container.getClient());
        FlinkV1ComputePool pool =
                new FlinkV1ComputePool()
                        .cell(cell)
                        .metadata(
                                new ApisMetaV1ObjectMeta()
                                        .org(org)
                                        .environment(env)
                                        .name(computePool))
                        .spec(
                                new FlinkV1ComputePoolSpec()
                                        .runtimeRef(
                                                new FlinkV1RuntimeRef().name("flink-test-runtime"))
                                        .resources(new FlinkV1ComputePoolResources().cfuLimit(1)));
        flinkV1Api.createFlinkV1ComputePool(env, org, pool);
    }

    /** Get a job by name. */
    public static FlinkV1Job getJob(
            ApiServerContainer container, String org, String env, String jobName) throws Exception {
        FlinkV1Api flinkV1Api = new FlinkV1Api(container.getClient());
        return flinkV1Api.readFlinkV1Job(env, jobName, org, null);
    }

    /** Create a job with the given arguments. */
    public static void createJob(
            ApiServerContainer container,
            String cell,
            String org,
            String env,
            String computePool,
            String jobName)
            throws Exception {
        FlinkV1Api flinkV1Api = new FlinkV1Api(container.getClient());
        FlinkV1Job job =
                new FlinkV1Job()
                        .cell(cell)
                        .metadata(
                                new ApisMetaV1ObjectMeta().org(org).environment(env).name(jobName))
                        .spec(
                                new FlinkV1JobSpec()
                                        .artifactRef(
                                                new ApisMetaV1EnvironmentLocalObjectReference()
                                                        .name("whatever.jar"))
                                        .computePoolRef(
                                                new ApisMetaV1EnvironmentLocalObjectReference()
                                                        .name(computePool))
                                        .jobManagerRef(
                                                new ApisMetaV1EnvironmentLocalObjectReference()
                                                        .name("jobmanager-test")));
        flinkV1Api.createFlinkV1Job(env, org, job);
    }

    /** List all UDF tasks in an org and env with the particular passed status. */
    public static Collection<ComputeV1FlinkUdfTask> listUdfTasksWithStatus(
            ApiServerContainer container, String org, String env, String status) throws Exception {
        try {
            return new HashSet<>(
                    container
                            .getComputeV1Api()
                            .listComputeV1FlinkUdfTasks(
                                    env, org, null, null, "status.phase=" + status, null, 1)
                            .getItems());
        } catch (ApiException e) {
            throw new Exception(e.getResponseBody(), e);
        }
    }
}
