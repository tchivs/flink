/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.util.IOUtils;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * This class encapsulates the runtime for interacting with remote UDFs, e.g. to call the remote
 * method.
 */
public class RemoteUdfRuntime implements AutoCloseable {
    private RemoteUdfRuntime(
            RemoteUdfSerialization remoteUdfSerialization,
            RemoteUdfGatewayConnection remoteUdfGatewayConnection,
            List<String> functionInstanceIds) {
        this.remoteUdfSerialization = remoteUdfSerialization;
        this.remoteUdfGatewayConnection = remoteUdfGatewayConnection;
        this.functionInstanceIds = functionInstanceIds;
    }

    /** Serialization methods. */
    private final RemoteUdfSerialization remoteUdfSerialization;
    /** Connection to the UDF remote service gateway. */
    private final RemoteUdfGatewayConnection remoteUdfGatewayConnection;
    /** List of IDs for function that were create by and are known to this runtime. */
    private final List<String> functionInstanceIds;

    /**
     * Calls the remote UDF.
     *
     * @param args the call arguments.
     * @return the return value.
     * @throws Exception on any error, e.g. from the connection or from the UDF code invocation.
     */
    public Object callRemoteUdf(Object[] args) throws Exception {
        String functionInstanceId = functionInstanceIds.get(0);
        ByteString serializedArguments = remoteUdfSerialization.serializeArguments(args);
        UdfGatewayOuterClass.InvokeResponse invokeResponse =
                remoteUdfGatewayConnection
                        .getUdfGateway()
                        .invoke(
                                UdfGatewayOuterClass.InvokeRequest.newBuilder()
                                        .setFuncInstanceId(functionInstanceId)
                                        .setPayload(serializedArguments)
                                        .build());

        checkAndHandleError(invokeResponse);
        return remoteUdfSerialization.deserializeReturnValue(invokeResponse.getPayload());
    }

    /**
     * Opens the runtime. This includes opening a connection to the gateway.
     *
     * @param udfGatewayTarget the gateway target address (e.g. localhost:5001).
     * @param remoteUdfSpec the specification of the UDF.
     * @return an open runtime.
     * @throws Exception on any error, e.g. from the connection or failing to create function
     *     instances.
     */
    public static RemoteUdfRuntime open(String udfGatewayTarget, RemoteUdfSpec remoteUdfSpec)
            throws Exception {

        RemoteUdfSerialization remoteUdfSerialization =
                new RemoteUdfSerialization(
                        remoteUdfSpec.createReturnTypeSerializer(),
                        remoteUdfSpec.createArgumentSerializers());

        RemoteUdfGatewayConnection remoteUdfGatewayConnection =
                RemoteUdfGatewayConnection.open(udfGatewayTarget);
        try {
            UdfGatewayOuterClass.CreateInstancesResponse createInstancesResponse =
                    remoteUdfGatewayConnection
                            .getUdfGateway()
                            .createInstances(
                                    // Create one instance of the specified function
                                    UdfGatewayOuterClass.CreateInstancesRequest.newBuilder()
                                            .setFuncId(remoteUdfSpec.getFunctionId())
                                            .setPayload(
                                                    remoteUdfSerialization.serializeRemoteUdfSpec(
                                                            remoteUdfSpec))
                                            .setNumInstances(1)
                                            .build());

            checkAndHandleError(createInstancesResponse);

            List<String> functionInstanceIds =
                    new ArrayList<>(createInstancesResponse.getFuncInstanceIdsList());

            return new RemoteUdfRuntime(
                    remoteUdfSerialization, remoteUdfGatewayConnection, functionInstanceIds);
        } catch (Exception ex) {
            // Cleanup on exception.
            IOUtils.closeQuietly(remoteUdfGatewayConnection);
            throw ex;
        }
    }

    /**
     * Closes the runtime, including the gateway connection.
     *
     * @throws Exception on any error.
     */
    @Override
    public void close() throws Exception {
        UdfGatewayOuterClass.DeleteInstancesResponse deleteInstancesResponse =
                remoteUdfGatewayConnection
                        .getUdfGateway()
                        .deleteInstances(
                                UdfGatewayOuterClass.DeleteInstancesRequest.newBuilder()
                                        .addAllFuncInstanceIds(functionInstanceIds)
                                        .build());

        checkAndHandleError(deleteInstancesResponse);

        remoteUdfGatewayConnection.close();
    }

    private static void checkAndHandleError(UdfGatewayOuterClass.InvokeResponse response)
            throws RemoteUdfException {
        if (response.hasError()) {
            throwForError(response.getError());
        }
    }

    private static void checkAndHandleError(UdfGatewayOuterClass.DeleteInstancesResponse response)
            throws RemoteUdfException {
        if (response.hasError()) {
            throwForError(response.getError());
        }
    }

    private static void checkAndHandleError(UdfGatewayOuterClass.CreateInstancesResponse response)
            throws RemoteUdfException {
        if (response.hasError()) {
            throwForError(response.getError());
        }
    }

    private static void throwForError(UdfGatewayOuterClass.Error error) throws RemoteUdfException {
        throw new RemoteUdfException(
                error.getMessage(), error.getCode(), error.getPayload().toByteArray());
    }
}
