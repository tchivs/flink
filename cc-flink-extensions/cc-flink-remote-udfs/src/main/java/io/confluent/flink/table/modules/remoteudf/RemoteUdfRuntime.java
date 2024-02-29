/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;
import io.confluent.flink.apiserver.client.ApiClient;
import io.confluent.flink.apiserver.client.ApiException;
import io.confluent.flink.apiserver.client.ComputeV1alphaApi;
import io.confluent.flink.apiserver.client.model.ComputeV1alphaFlinkUdfTask;
import io.confluent.secure.compute.gateway.v1.Error;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionRequest;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionResponse;

import java.time.Duration;
import java.util.Map;

import static io.confluent.flink.apiserver.client.model.ComputeV1alphaFlinkUdfTaskStatus.PhaseEnum.RUNNING;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.getUdfTaskFromSpec;

/**
 * This class encapsulates the runtime for interacting with remote UDFs, e.g. to call the remote
 * method.
 */
public class RemoteUdfRuntime implements AutoCloseable {

    private static final int POLL_INTERVAL_MS = 500;

    private RemoteUdfRuntime(
            RemoteUdfSerialization remoteUdfSerialization,
            RemoteUdfGatewayConnection remoteUdfGatewayConnection,
            String functionInstanceName,
            ApiClient apiClient,
            ComputeV1alphaFlinkUdfTask udfTask) {
        this.remoteUdfSerialization = remoteUdfSerialization;
        this.remoteUdfGatewayConnection = remoteUdfGatewayConnection;
        this.functionInstanceName = functionInstanceName;
        this.apiClient = apiClient;
        this.udfTask = udfTask;
    }

    /** Serialization methods. */
    private final RemoteUdfSerialization remoteUdfSerialization;
    /** Connection to the UDF remote service gateway. */
    private final RemoteUdfGatewayConnection remoteUdfGatewayConnection;
    /** The id of the function that was created by and is known to this runtime. */
    private final String functionInstanceName;

    private final ApiClient apiClient;

    private final ComputeV1alphaFlinkUdfTask udfTask;

    /**
     * Calls the remote UDF.
     *
     * @param args the call arguments.
     * @return the return value.
     * @throws Exception on any error, e.g. from the connection or from the UDF code invocation.
     */
    public Object callRemoteUdf(Object[] args) throws Exception {
        ByteString serializedArguments = remoteUdfSerialization.serializeArguments(args);
        InvokeFunctionResponse invokeResponse =
                remoteUdfGatewayConnection
                        .getUdfGateway()
                        .invokeFunction(
                                InvokeFunctionRequest.newBuilder()
                                        .setFuncInstanceName(functionInstanceName)
                                        .setPayload(serializedArguments)
                                        .build());

        checkAndHandleError(invokeResponse);
        return remoteUdfSerialization.deserializeReturnValue(invokeResponse.getPayload());
    }

    /**
     * Opens the runtime. This includes opening a connection to the gateway.
     *
     * @param confMap
     * @param remoteUdfSpec the specification of the UDF.
     * @return an open runtime.
     * @throws Exception on any error, e.g. from the connection or failing to create function
     *     instances.
     */
    public static RemoteUdfRuntime open(Map<String, String> confMap, RemoteUdfSpec remoteUdfSpec)
            throws Exception {
        RemoteUdfSerialization remoteUdfSerialization =
                new RemoteUdfSerialization(
                        remoteUdfSpec.createReturnTypeSerializer(),
                        remoteUdfSpec.createArgumentSerializers());

        RemoteUdfGatewayConnection remoteUdfGatewayConnection = null;
        try {
            ComputeV1alphaFlinkUdfTask udfTask =
                    getUdfTaskFromSpec(confMap, remoteUdfSpec, remoteUdfSerialization);

            ApiClient apiClient = getApiClient(confMap);
            ComputeV1alphaApi computeV1alphaApi = new ComputeV1alphaApi(apiClient);
            computeV1alphaApi.createComputeV1alphaFlinkUdfTask(
                    udfTask.getMetadata().getEnvironment(),
                    udfTask.getMetadata().getOrg(),
                    udfTask);

            // TODO FRT-353 integrate with Watch API
            Deadline deadline = Deadline.fromNow(Duration.ofMinutes(2));
            while (deadline.hasTimeLeft()) {
                udfTask = getUdfTask(computeV1alphaApi, udfTask);
                if (udfTask.getStatus().getPhase() == RUNNING) {
                    remoteUdfGatewayConnection = RemoteUdfGatewayConnection.open(udfTask);
                    return new RemoteUdfRuntime(
                            remoteUdfSerialization,
                            remoteUdfGatewayConnection,
                            udfTask.getMetadata().getName(),
                            apiClient,
                            udfTask);
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

    private static ComputeV1alphaFlinkUdfTask getUdfTask(
            ComputeV1alphaApi computeV1alphaApi, ComputeV1alphaFlinkUdfTask udfTask)
            throws ApiException {
        return computeV1alphaApi.readComputeV1alphaFlinkUdfTask(
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
    }

    /** Deletes the UdfTask from the ApiServer. */
    private static void deleteUdfTask(ApiClient apiClient, ComputeV1alphaFlinkUdfTask udfTask)
            throws ApiException {
        if (udfTask == null) {
            ComputeV1alphaApi computeV1alphaApi = new ComputeV1alphaApi(apiClient);
            computeV1alphaApi.deleteComputeV1alphaFlinkUdfTask(
                    udfTask.getMetadata().getEnvironment(),
                    udfTask.getMetadata().getName(),
                    udfTask.getMetadata().getOrg());
        }
    }

    /** Returns the ApiClient for the given configuration. */
    private static ApiClient getApiClient(Map<String, String> config) {
        String apiServerEndpoint =
                config.getOrDefault(CONFLUENT_CONFLUENT_REMOTE_UDF_APISERVER.key(), "");
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
}
