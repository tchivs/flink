/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCache;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.confluent.flink.apiserver.client.ApiClient;
import io.confluent.flink.apiserver.client.ApiException;
import io.confluent.flink.apiserver.client.ComputeV1Api;
import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTask;
import io.confluent.flink.udf.adapter.api.RemoteUdfSerialization;
import io.confluent.flink.udf.adapter.api.RemoteUdfSpec;
import io.confluent.flink.udf.adapter.api.ThreadLocalRemoteUdfSerialization;
import io.confluent.flink.udf.adapter.api.UdfSerialization;
import io.confluent.secure.compute.gateway.v1.Error;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionRequest;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionResponse;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTaskStatus.PhaseEnum.RUNNING;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_ASYNC_ENABLED;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.getUdfTaskFromSpec;
import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.apache.flink.shaded.guava31.com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * This class encapsulates the runtime for interacting with remote UDFs, e.g. to call the remote
 * method.
 */
public class RemoteUdfRuntime implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteUdfRuntime.class);

    public static final Metadata.Key<String> AUTH_METADATA =
            Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);

    private static final int POLL_INTERVAL_MS = 500;

    private RemoteUdfRuntime(
            UdfSerialization udfSerialization,
            RemoteUdfGatewayConnection remoteUdfGatewayConnection,
            String functionInstanceName,
            ApiClient apiClient,
            ComputeV1FlinkUdfTask udfTask,
            RemoteUdfMetrics metrics,
            KafkaCredentialsCache credentialsCache,
            JobID jobID) {
        this.udfSerialization = udfSerialization;
        this.remoteUdfGatewayConnection = remoteUdfGatewayConnection;
        this.functionInstanceName = functionInstanceName;
        this.apiClient = apiClient;
        this.udfTask = udfTask;
        this.metrics = metrics;
        this.credentialsCache = credentialsCache;
        this.jobID = jobID;
    }

    /** Serialization methods. */
    private final UdfSerialization udfSerialization;
    /** Connection to the UDF remote service gateway. */
    private final RemoteUdfGatewayConnection remoteUdfGatewayConnection;
    /** The id of the function that was created by and is known to this runtime. */
    private final String functionInstanceName;

    /** Api client used to provision and deprovision compute tasks. */
    private final ApiClient apiClient;

    /** The UDF task ready to take calls. */
    private final ComputeV1FlinkUdfTask udfTask;

    /** Metrics object used to track events. */
    private final RemoteUdfMetrics metrics;

    /** Where we store tokens used to access the compute platform. */
    private final KafkaCredentialsCache credentialsCache;

    /** The job id of the current job running this UDF. */
    private final JobID jobID;

    /**
     * Calls the remote UDF.
     *
     * @param args the call arguments.
     * @return the return value.
     * @throws Exception on any error, e.g. from the connection or from the UDF code invocation.
     */
    public Object callRemoteUdf(Object[] args) throws Exception {
        ByteString serializedArguments = udfSerialization.serializeArguments(args);
        metrics.bytesToUdf(serializedArguments.size());
        InvokeFunctionRequest request =
                InvokeFunctionRequest.newBuilder()
                        .setFuncInstanceName(functionInstanceName)
                        .setPayload(serializedArguments)
                        .build();
        // TRAFFIC-11799: Avoid transient connectivity issues for EA
        InvokeFunctionResponse invokeResponse =
                remoteUdfGatewayConnection
                        .getCallWithRetry()
                        .call(
                                () ->
                                        remoteUdfGatewayConnection
                                                .getUdfGateway()
                                                .withDeadlineAfter(
                                                        remoteUdfGatewayConnection
                                                                .getDeadlineSeconds(),
                                                        TimeUnit.SECONDS)
                                                .withCallCredentials(
                                                        new UdfCallCredentials(
                                                                credentialsCache, jobID))
                                                .invokeFunction(request));

        checkAndHandleError(invokeResponse);
        ByteString serializedResult = invokeResponse.getPayload();
        metrics.bytesFromUdf(serializedResult.size());
        return udfSerialization.deserializeReturnValue(serializedResult);
    }

    // Note that unless we get the gateway library to use shaded Guava, we need to reference
    // the fully qualified name.  Otherwise, style tests will ask us to import only the shaded
    // version.
    public com.google.common.util.concurrent.ListenableFuture<Object> callRemoteUdfAsync(
            Object[] args, Executor executor) throws Exception {
        ByteString serializedArguments = udfSerialization.serializeArguments(args);
        metrics.bytesToUdf(serializedArguments.size());
        InvokeFunctionRequest request =
                InvokeFunctionRequest.newBuilder()
                        .setFuncInstanceName(functionInstanceName)
                        .setPayload(serializedArguments)
                        .build();

        com.google.common.util.concurrent.ListenableFuture<InvokeFunctionResponse> responseFuture =
                remoteUdfGatewayConnection
                        .getAsyncUdfGateway()
                        .withDeadlineAfter(
                                remoteUdfGatewayConnection.getDeadlineSeconds(), TimeUnit.SECONDS)
                        .withCallCredentials(new UdfCallCredentials(credentialsCache, jobID))
                        .withExecutor(executor)
                        .invokeFunction(request);
        return com.google.common.util.concurrent.Futures.transform(
                responseFuture,
                invokeResponse -> {
                    try {
                        checkAndHandleError(invokeResponse);
                        ByteString serializedResult = invokeResponse.getPayload();
                        metrics.bytesFromUdf(serializedResult.size());
                        return udfSerialization.deserializeReturnValue(serializedResult);
                    } catch (Throwable t) {
                        // Wrap anything in a RuntimeException so it can be bubbled up
                        throw new RuntimeException(t);
                    }
                },
                directExecutor());
    }

    /**
     * Opens the runtime. This includes opening a connection to the gateway.
     *
     * @param config the configuration for the runtime.
     * @param remoteUdfSpec the specification of the UDF.
     * @return an open runtime.
     * @throws Exception on any error, e.g. from the connection or failing to create function
     *     instances.
     */
    public static RemoteUdfRuntime open(
            Configuration config,
            RemoteUdfSpec remoteUdfSpec,
            RemoteUdfMetrics metrics,
            KafkaCredentialsCache credentialsCache,
            JobID jobID)
            throws Exception {
        UdfSerialization udfSerialization =
                config.get(CONFLUENT_REMOTE_UDF_ASYNC_ENABLED)
                        ? new ThreadLocalRemoteUdfSerialization(
                                remoteUdfSpec.createReturnTypeSerializer(),
                                remoteUdfSpec.createArgumentSerializers())
                        : new RemoteUdfSerialization(
                                remoteUdfSpec.createReturnTypeSerializer(),
                                remoteUdfSpec.createArgumentSerializers());

        RemoteUdfGatewayConnection remoteUdfGatewayConnection = null;
        try {
            ComputeV1FlinkUdfTask udfTask =
                    getUdfTaskFromSpec(config, remoteUdfSpec, udfSerialization);

            ApiClient apiClient = getApiClient(config);
            ComputeV1Api computeV1Api = new ComputeV1Api(apiClient);
            computeV1Api.createComputeV1FlinkUdfTask(
                    udfTask.getMetadata().getEnvironment(),
                    udfTask.getMetadata().getOrg(),
                    udfTask);

            // TODO FRT-353 integrate with Watch API
            Deadline deadline = Deadline.fromNow(Duration.ofMinutes(2));
            while (deadline.hasTimeLeft()) {
                udfTask = getUdfTask(computeV1Api, udfTask);
                if (udfTask.getStatus().getPhase() == RUNNING) {
                    remoteUdfGatewayConnection = RemoteUdfGatewayConnection.open(udfTask, config);
                    return new RemoteUdfRuntime(
                            udfSerialization,
                            remoteUdfGatewayConnection,
                            udfTask.getMetadata().getName(),
                            apiClient,
                            udfTask,
                            metrics,
                            credentialsCache,
                            jobID);
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }
            throw new RuntimeException(
                    "UdfTask "
                            + udfTask.getMetadata().getName()
                            + "could not be started. Timeout exceeded.");
        } catch (Exception ex) {
            // Cleanup on exception.
            IOUtils.closeQuietly(remoteUdfGatewayConnection);
            throw ex;
        }
    }

    private static ComputeV1FlinkUdfTask getUdfTask(
            ComputeV1Api computeV1Api, ComputeV1FlinkUdfTask udfTask) throws ApiException {
        return computeV1Api.readComputeV1FlinkUdfTask(
                udfTask.getMetadata().getEnvironment(),
                udfTask.getMetadata().getName(),
                udfTask.getMetadata().getOrg(),
                null);
    }

    /**
     * Cleans up the runtime, including closing gateway connection and deleting UdfTask from the
     * ApiServer.
     *
     * @throws Exception on any error.
     */
    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(remoteUdfGatewayConnection);
        deleteUdfTask(apiClient, udfTask);
        udfSerialization.close();
    }

    /** Deletes the UdfTask from the ApiServer. */
    private static void deleteUdfTask(ApiClient apiClient, ComputeV1FlinkUdfTask udfTask)
            throws ApiException {
        ComputeV1Api computeV1Api = new ComputeV1Api(apiClient);
        computeV1Api.deleteComputeV1FlinkUdfTask(
                udfTask.getMetadata().getEnvironment(),
                udfTask.getMetadata().getName(),
                udfTask.getMetadata().getOrg());
    }

    /** Returns the ApiClient for the given configuration. */
    private static ApiClient getApiClient(Configuration config) {
        String apiServerEndpoint = config.getString(CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER);
        Preconditions.checkArgument(
                !apiServerEndpoint.isEmpty(), "ApiServer target not configured!");
        ApiClient apiClient = new ApiClient().setBasePath(apiServerEndpoint);
        // set the client just in case we use the default ctors somewhere
        io.confluent.flink.apiserver.client.Configuration.setDefaultApiClient(apiClient);
        return apiClient;
    }

    /** Checks the response for an error and throws an exception if there is one. */
    private static void checkAndHandleError(InvokeFunctionResponse response)
            throws RemoteUdfException {
        if (response.hasError()) {
            throwForError(response.getError());
        }
    }

    /** Throws a RemoteUdfException for the given error. */
    private static void throwForError(Error error) throws RemoteUdfException {
        throw new RemoteUdfException(
                error.getMessage(), error.getCode(), error.getMessageBytes().toByteArray());
    }

    private static class UdfCallCredentials extends CallCredentials {
        private final KafkaCredentialsCache credentialsCache;
        private final JobID jobId;

        public UdfCallCredentials(KafkaCredentialsCache credentialsCache, JobID jobId) {
            super();
            this.credentialsCache = credentialsCache;
            this.jobId = jobId;
        }

        /** Refreshes the token from the cache. */
        @Override
        public void applyRequestMetadata(
                RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
            appExecutor.execute(
                    () -> {
                        try {
                            Optional<KafkaCredentials> credentials =
                                    credentialsCache.getCredentials(jobId);
                            String token =
                                    credentials
                                            .flatMap(KafkaCredentials::getUdfDpatToken)
                                            .orElseThrow(
                                                    () ->
                                                            new RuntimeException(
                                                                    "No token available"));

                            Metadata metadata = new Metadata();
                            metadata.put(AUTH_METADATA, "Bearer " + token);
                            applier.apply(metadata);
                        } catch (Exception e) {
                            LOG.error("Error fetching credentials", e);
                            applier.fail(Status.UNAUTHENTICATED.withCause(e));
                        }
                    });
        }

        @Override
        public void thisUsesUnstableApi() {}
    }
}
