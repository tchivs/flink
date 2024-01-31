/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.util.IOUtils;

import com.google.protobuf.ByteString;
import io.confluent.secure.compute.gateway.v1.CreateInstanceRequest;
import io.confluent.secure.compute.gateway.v1.CreateInstanceResponse;
import io.confluent.secure.compute.gateway.v1.DeleteInstanceRequest;
import io.confluent.secure.compute.gateway.v1.DeleteInstanceResponse;
import io.confluent.secure.compute.gateway.v1.Error;
import io.confluent.secure.compute.gateway.v1.FunctionInstanceEntryPoint;
import io.confluent.secure.compute.gateway.v1.FunctionInstanceMetadata;
import io.confluent.secure.compute.gateway.v1.FunctionInstanceSpec;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionRequest;
import io.confluent.secure.compute.gateway.v1.InvokeFunctionResponse;

/**
 * This class encapsulates the runtime for interacting with remote UDFs, e.g. to call the remote
 * method.
 */
public class RemoteUdfRuntime implements AutoCloseable {
    private RemoteUdfRuntime(
            RemoteUdfSerialization remoteUdfSerialization,
            RemoteUdfGatewayConnection remoteUdfGatewayConnection,
            String functionInstanceName) {
        this.remoteUdfSerialization = remoteUdfSerialization;
        this.remoteUdfGatewayConnection = remoteUdfGatewayConnection;
        this.functionInstanceName = functionInstanceName;
    }

    /** Serialization methods. */
    private final RemoteUdfSerialization remoteUdfSerialization;
    /** Connection to the UDF remote service gateway. */
    private final RemoteUdfGatewayConnection remoteUdfGatewayConnection;
    /** The id of the function that was created by and is known to this runtime. */
    private final String functionInstanceName;

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
     * @param udfGatewayTarget the gateway target address (e.g. localhost:5001).
     * @param remoteUdfSpec the specification of the UDF.
     * @return an open runtime.
     * @throws Exception on any error, e.g. from the connection or failing to create function
     *     instances.
     */
    public static RemoteUdfRuntime open(String udfGatewayTarget, RemoteUdfSpec remoteUdfSpec)
            throws Exception {
        // TODO: FRT-321 Replace this with UDFTask call to apiServer
        RemoteUdfSerialization remoteUdfSerialization =
                new RemoteUdfSerialization(
                        remoteUdfSpec.createReturnTypeSerializer(),
                        remoteUdfSpec.createArgumentSerializers());

        RemoteUdfGatewayConnection remoteUdfGatewayConnection =
                RemoteUdfGatewayConnection.open(udfGatewayTarget);
        try {
            FunctionInstanceSpec functionSpec =
                    FunctionInstanceSpec.newBuilder()
                            .setEntryPoint(
                                    FunctionInstanceEntryPoint.newBuilder()
                                            .setOpenPayload(
                                                    remoteUdfSerialization.serializeRemoteUdfSpec(
                                                            remoteUdfSpec))
                                            .build())
                            .build();
            CreateInstanceResponse createInstancesResponse =
                    remoteUdfGatewayConnection
                            .getUdfGateway()
                            .createInstance(
                                    // Create one instance of the specified function
                                    CreateInstanceRequest.newBuilder()
                                            .setMetadata(
                                                    FunctionInstanceMetadata.newBuilder()
                                                            .setName(remoteUdfSpec.getFunctionId()))
                                            .setSpec(functionSpec)
                                            .build());

            checkAndHandleError(createInstancesResponse);

            return new RemoteUdfRuntime(
                    remoteUdfSerialization,
                    remoteUdfGatewayConnection,
                    remoteUdfSpec.getFunctionId());
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
        DeleteInstanceResponse deleteInstancesResponse =
                remoteUdfGatewayConnection
                        .getUdfGateway()
                        .deleteInstance(
                                DeleteInstanceRequest.newBuilder()
                                        .setMetadata(
                                                FunctionInstanceMetadata.newBuilder()
                                                        .setName(functionInstanceName)
                                                        .build())
                                        .build());

        checkAndHandleError(deleteInstancesResponse);

        remoteUdfGatewayConnection.close();
    }

    private static void checkAndHandleError(InvokeFunctionResponse response)
            throws RemoteUdfException {
        if (response.hasError()) {
            throwForError(response.getError());
        }
    }

    private static void checkAndHandleError(DeleteInstanceResponse response)
            throws RemoteUdfException {
        if (response.hasError()) {
            throwForError(response.getError());
        }
    }

    private static void checkAndHandleError(CreateInstanceResponse response)
            throws RemoteUdfException {
        if (response.hasError()) {
            throwForError(response.getError());
        }
    }

    private static void throwForError(Error error) throws RemoteUdfException {
        throw new RemoteUdfException(
                error.getMessage(), error.getCode(), error.getMessageBytes().toByteArray());
    }
}
