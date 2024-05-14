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

import com.google.protobuf.ByteString;
import io.confluent.flink.apiserver.client.ApiClient;
import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTask;
import io.confluent.flink.table.modules.remoteudf.utils.ApiServerUtils;
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
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_ASYNC_ENABLED;
import static io.confluent.flink.table.modules.remoteudf.utils.ApiServerUtils.createApiServerUdfTask;
import static io.confluent.flink.table.modules.remoteudf.utils.ApiServerUtils.generateUdfTaskFromSpec;
import static io.confluent.flink.table.modules.remoteudf.utils.ApiServerUtils.updateApiServerJobWithUdfTaskOwnerRef;
import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static org.apache.flink.shaded.guava31.com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * This class encapsulates the runtime for interacting with remote UDFs, e.g. to call the remote
 * method.
 */
public class RemoteUdfRuntime implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteUdfRuntime.class);

    /** Configuration for the runtime. */
    private final Configuration config;

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

    /** Metadata key for the authorization token. */
    public static final Metadata.Key<String> AUTH_METADATA =
            Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);

    /** The interval in milliseconds to poll for the UDF task status. */
    private static final int POLL_INTERVAL_MS = 500;

    /** The maximum duration to wait for the UDF task to be provisioned. */
    private static final Duration MAX_UDFTASK_PROVISION_DURATION = Duration.ofMinutes(2);

    /** The job id of the current job running this UDF. */
    private final JobID jobID;

    private RemoteUdfRuntime(
            Configuration config,
            UdfSerialization udfSerialization,
            RemoteUdfGatewayConnection remoteUdfGatewayConnection,
            String functionInstanceName,
            ApiClient apiClient,
            ComputeV1FlinkUdfTask udfTask,
            RemoteUdfMetrics metrics,
            KafkaCredentialsCache credentialsCache,
            JobID jobID) {
        this.config = config;
        this.udfSerialization = udfSerialization;
        this.remoteUdfGatewayConnection = remoteUdfGatewayConnection;
        this.functionInstanceName = functionInstanceName;
        this.apiClient = apiClient;
        this.udfTask = udfTask;
        this.metrics = metrics;
        this.credentialsCache = credentialsCache;
        this.jobID = jobID;
    }

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
        final UdfSerialization udfSerialization =
                config.get(CONFLUENT_REMOTE_UDF_ASYNC_ENABLED)
                        ? new ThreadLocalRemoteUdfSerialization(
                                remoteUdfSpec.createReturnTypeSerializer(),
                                remoteUdfSpec.createArgumentSerializers())
                        : new RemoteUdfSerialization(
                                remoteUdfSpec.createReturnTypeSerializer(),
                                remoteUdfSpec.createArgumentSerializers());

        RemoteUdfGatewayConnection remoteUdfGatewayConnection = null;
        try {
            final ApiClient apiClient = ApiServerUtils.getApiClient(config);
            final ComputeV1FlinkUdfTask udfTask =
                    createApiServerUdfTask(
                            config,
                            apiClient,
                            generateUdfTaskFromSpec(
                                    config, remoteUdfSpec, udfSerialization, jobID));
            updateApiServerJobWithUdfTaskOwnerRef(config, apiClient, udfTask);

            // TODO FRT-353 integrate with Watch API
            Deadline deadline = Deadline.fromNow(MAX_UDFTASK_PROVISION_DURATION);
            while (deadline.hasTimeLeft()) {
                final ComputeV1FlinkUdfTask currentUdfTask =
                        ApiServerUtils.getUdfTask(config, apiClient, udfTask);
                if (currentUdfTask.getStatus().getPhase() == RUNNING) {
                    metrics.provisionMs(
                            MAX_UDFTASK_PROVISION_DURATION.toMillis()
                                    - deadline.timeLeft().toMillis());
                    remoteUdfGatewayConnection =
                            RemoteUdfGatewayConnection.open(currentUdfTask, config);
                    return new RemoteUdfRuntime(
                            config,
                            udfSerialization,
                            remoteUdfGatewayConnection,
                            currentUdfTask.getMetadata().getName(),
                            apiClient,
                            currentUdfTask,
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

    /**
     * Cleans up the runtime, including closing gateway connection and deleting UdfTask from the
     * ApiServer.
     *
     * @throws Exception on any error.
     */
    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(remoteUdfGatewayConnection);
        ApiServerUtils.deleteUdfTask(config, apiClient, udfTask);
        udfSerialization.close();
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
