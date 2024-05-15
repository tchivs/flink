/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.apiserver.client.ApiClient;
import io.confluent.flink.apiserver.client.ApiException;
import io.confluent.flink.apiserver.client.ComputeV1Api;
import io.confluent.flink.apiserver.client.FlinkV1Api;
import io.confluent.flink.apiserver.client.model.ApisMetaV1ObjectMeta;
import io.confluent.flink.apiserver.client.model.ApisMetaV1OwnerReference;
import io.confluent.flink.apiserver.client.model.ComputeV1Artifact;
import io.confluent.flink.apiserver.client.model.ComputeV1EntryPoint;
import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTask;
import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTaskSpec;
import io.confluent.flink.apiserver.client.model.FlinkV1Job;
import io.confluent.flink.udf.adapter.api.RemoteUdfSpec;
import io.confluent.flink.udf.adapter.api.UdfSerialization;

import java.io.IOException;
import java.util.Base64;
import java.util.UUID;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_APISERVER;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_APISERVER_RETRY_BACKOFF_MS;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_APISERVER_RETRY_MAX_ATTEMPTS;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.JOB_NAME;

/** Common utilities for the ApiServer. */
public class ApiServerUtils {

    public static final String LABEL_JOB_ID = "confluent.io/job-id";

    /**
     * Generate a UdfTask object from the given RemoteUdfSpec.
     *
     * @param config Configuration
     * @param remoteUdfSpec RemoteUdfSpec
     * @param udfSerialization UdfSerialization
     * @param jobID JobID
     * @return ComputeV1FlinkUdfTask
     * @throws IOException If the RemoteUdfSpec cannot be serialized
     */
    public static ComputeV1FlinkUdfTask generateUdfTaskFromSpec(
            Configuration config,
            RemoteUdfSpec remoteUdfSpec,
            UdfSerialization udfSerialization,
            JobID jobID)
            throws IOException {
        String pluginId = config.getString(CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID);
        String versionId = config.getString(CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID);
        Preconditions.checkArgument(!pluginId.isEmpty(), "PluginId must be set");
        Preconditions.checkArgument(!versionId.isEmpty(), "VersionId must be set");

        String udfTaskName = "udf-tm-" + UUID.randomUUID();
        ComputeV1FlinkUdfTask udfTask = new ComputeV1FlinkUdfTask();
        ComputeV1FlinkUdfTaskSpec udfTaskSpec = new ComputeV1FlinkUdfTaskSpec();

        // Metadata must contain Org, Env, and Name
        ApisMetaV1ObjectMeta udfTaskMeta = new ApisMetaV1ObjectMeta();
        udfTaskMeta.setName(udfTaskName);
        udfTaskMeta.setOrg(remoteUdfSpec.getOrganization());
        udfTaskMeta.setEnvironment(remoteUdfSpec.getEnvironment());
        udfTaskMeta.setLabels(ImmutableMap.of(LABEL_JOB_ID, jobID.toHexString()));

        // Entrypoint must contain className, open and close Payloads
        ComputeV1EntryPoint udfTaskEntryPoint = new ComputeV1EntryPoint();
        udfTaskEntryPoint.setClassName("io.confluent.flink.udf.adapter.ScalarFunctionHandler");
        udfTaskEntryPoint.setOpenPayload(
                udfSerialization
                        .serializeRemoteUdfSpec(remoteUdfSpec)
                        .toByteArray()); // to be removed
        // Unused at the moment -- pass something until platform supports empty payloads
        udfTaskEntryPoint.setClosePayload(Base64.getEncoder().encode("bye".getBytes()));

        // Artifact must contain name, scope, pluginId, versionId, inClassPath, and orgOpts
        ComputeV1Artifact udfTaskArtifact = new ComputeV1Artifact();
        udfTaskArtifact.setName("udf-task");
        udfTaskArtifact.setScope(ComputeV1Artifact.ScopeEnum.ORG);
        udfTaskArtifact.setPluginId(remoteUdfSpec.getPluginId());
        udfTaskArtifact.setVersionId(remoteUdfSpec.getPluginVersionId());
        udfTaskArtifact.setInClassPath(true);

        ComputeV1Artifact udfTaskArtifactInternal = new ComputeV1Artifact();
        udfTaskArtifactInternal.setName("udf-shim-internal");
        udfTaskArtifactInternal.setScope(ComputeV1Artifact.ScopeEnum.INTERNAL);
        udfTaskArtifactInternal.setPluginId(pluginId);
        udfTaskArtifactInternal.setVersionId(versionId);
        udfTaskArtifactInternal.setInClassPath(true);

        // Enrich UdfTaskSpec with Artifact and EntryPoint details
        udfTaskSpec.addArtifactsItem(udfTaskArtifact);
        udfTaskSpec.addArtifactsItem(udfTaskArtifactInternal);
        udfTaskSpec.setEntryPoint(udfTaskEntryPoint);
        // Assign the UdfTaskSpec to the UdfTask
        udfTask.setSpec(udfTaskSpec);
        // Assign the Metadata to the UdfTask
        udfTask.setMetadata(udfTaskMeta);
        return udfTask;
    }

    /**
     * Retrieve the UdfTask from the API server.
     *
     * @param config The Configuration to use to connect to the API server
     * @param apiClient The ApiClient to use to connect to the API server
     * @param udfTask The UdfTask to get from the API server
     * @return The UdfTask from the API server
     * @throws ApiException If the API server returns an error
     */
    public static ComputeV1FlinkUdfTask getUdfTask(
            Configuration config, ApiClient apiClient, ComputeV1FlinkUdfTask udfTask)
            throws ApiException {
        final ComputeV1Api computeV1Api = new ComputeV1Api(apiClient);
        final CallWithRetry apiserverRetryCall =
                new CallWithRetry(
                        config.getInteger(CONFLUENT_REMOTE_UDF_APISERVER_RETRY_MAX_ATTEMPTS),
                        config.getLong(CONFLUENT_REMOTE_UDF_APISERVER_RETRY_BACKOFF_MS));
        return apiserverRetryCall.call(
                () -> {
                    try {
                        return computeV1Api.readComputeV1FlinkUdfTask(
                                udfTask.getMetadata().getEnvironment(),
                                udfTask.getMetadata().getName(),
                                udfTask.getMetadata().getOrg(),
                                null);
                    } catch (ApiException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Connect to the API server and create a UdfTask.
     *
     * @param config The Configuration to use to connect to the API server
     * @param apiClient The ApiClient to use to connect to the API server
     * @param udfTask The UdfTask to create
     */
    public static ComputeV1FlinkUdfTask createApiServerUdfTask(
            Configuration config, ApiClient apiClient, ComputeV1FlinkUdfTask udfTask) {
        final ComputeV1Api computeV1Api = new ComputeV1Api(apiClient);
        final CallWithRetry apiserverRetryCall =
                new CallWithRetry(
                        config.getInteger(CONFLUENT_REMOTE_UDF_APISERVER_RETRY_MAX_ATTEMPTS),
                        config.getLong(CONFLUENT_REMOTE_UDF_APISERVER_RETRY_BACKOFF_MS));
        return apiserverRetryCall.call(
                () -> {
                    try {
                        return computeV1Api.createComputeV1FlinkUdfTask(
                                udfTask.getMetadata().getEnvironment(),
                                udfTask.getMetadata().getOrg(),
                                udfTask);
                    } catch (ApiException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Connect to the API server and delete the UdfTask.
     *
     * @param config The Configuration to use to connect to the API server
     * @param apiClient The ApiClient to use to connect to the API server
     * @param udfTask The UdfTask to delete
     * @throws ApiException If the API server returns an error
     */
    public static void deleteUdfTask(
            Configuration config, ApiClient apiClient, ComputeV1FlinkUdfTask udfTask)
            throws ApiException {
        final ComputeV1Api computeV1Api = new ComputeV1Api(apiClient);
        final CallWithRetry apiserverRetryCall =
                new CallWithRetry(
                        config.getInteger(CONFLUENT_REMOTE_UDF_APISERVER_RETRY_MAX_ATTEMPTS),
                        config.getLong(CONFLUENT_REMOTE_UDF_APISERVER_RETRY_BACKOFF_MS));
        apiserverRetryCall.call(
                () -> {
                    try {
                        return computeV1Api.deleteComputeV1FlinkUdfTask(
                                udfTask.getMetadata().getEnvironment(),
                                udfTask.getMetadata().getName(),
                                udfTask.getMetadata().getOrg());
                    } catch (ApiException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Get the ApiClient for the given Configuration.
     *
     * @param config The Configuration to use to connect to the API server
     * @return The ApiClient to use to connect to the API server
     */
    public static ApiClient getApiClient(Configuration config) {
        final String apiServerEndpoint = config.getString(CONFLUENT_REMOTE_UDF_APISERVER);
        Preconditions.checkArgument(
                !apiServerEndpoint.isEmpty(), "ApiServer target not configured!");
        final ApiClient apiClient = new ApiClient().setBasePath(apiServerEndpoint);
        // set the client just in case we use the default ctors somewhere
        io.confluent.flink.apiserver.client.Configuration.setDefaultApiClient(apiClient);
        return apiClient;
    }

    /**
     * Connect to the API server and update the Job with the job.name passed from configuration, as
     * the owner of the given UdfTask owner so that it can be garbage collected when the job is
     * deleted or terminated.
     *
     * @param config The Configuration to use to connect to the API server
     * @param apiClient The ApiClient to use to connect to the API server
     * @param udfTask The UdfTask that will be owned by the Job
     */
    public static void updateApiServerJobWithUdfTaskOwnerRef(
            Configuration config, ApiClient apiClient, ComputeV1FlinkUdfTask udfTask) {
        final FlinkV1Api flinkV1Api = new FlinkV1Api(apiClient);
        final ComputeV1Api computeV1Api = new ComputeV1Api(apiClient);
        final String jobName = config.getString(JOB_NAME);
        Preconditions.checkArgument(!jobName.isEmpty(), "Job Name must be set");
        final CallWithRetry apiserverRetryCall =
                new CallWithRetry(
                        config.getInteger(CONFLUENT_REMOTE_UDF_APISERVER_RETRY_MAX_ATTEMPTS),
                        config.getLong(CONFLUENT_REMOTE_UDF_APISERVER_RETRY_BACKOFF_MS));
        apiserverRetryCall.call(
                () -> {
                    try {
                        final FlinkV1Job flinkV1Job =
                                flinkV1Api.readFlinkV1Job(
                                        udfTask.getMetadata().getEnvironment(),
                                        jobName,
                                        udfTask.getMetadata().getOrg(),
                                        null);

                        udfTask.getMetadata()
                                .addOwnerReferencesItem(
                                        new ApisMetaV1OwnerReference()
                                                .apiVersion(flinkV1Job.getApiVersion())
                                                .kind(flinkV1Job.getKind())
                                                .name(flinkV1Job.getMetadata().getName())
                                                .uid(flinkV1Job.getMetadata().getUid()));

                        return computeV1Api.updateComputeV1FlinkUdfTask(
                                udfTask.getMetadata().getEnvironment(),
                                udfTask.getMetadata().getName(),
                                udfTask.getMetadata().getOrg(),
                                udfTask);
                    } catch (ApiException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
