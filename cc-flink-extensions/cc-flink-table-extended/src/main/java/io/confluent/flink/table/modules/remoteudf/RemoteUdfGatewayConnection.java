/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.util.Preconditions;

import io.confluent.secure.compute.gateway.v1.SecureComputeGatewayGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.Closeable;

/** Encapsulates a gRPC connection to communicate with remote UDF service gateway. */
public class RemoteUdfGatewayConnection implements Closeable {
    /** Target address for the udfGateway. */
    private final String udfGatewayTarget;

    /** The managed channel used to construct the udfGateway. */
    private ManagedChannel channel;

    /** Gateway to invoke remote UDFs. */
    private SecureComputeGatewayGrpc.SecureComputeGatewayBlockingStub udfGateway;

    private RemoteUdfGatewayConnection(
            String udfGatewayTarget,
            ManagedChannel channel,
            SecureComputeGatewayGrpc.SecureComputeGatewayBlockingStub udfGateway) {
        this.udfGatewayTarget = udfGatewayTarget;
        this.channel = channel;
        this.udfGateway = udfGateway;
    }

    /**
     * Opens a connection to the given target gateway.
     *
     * @param udfGatewayTarget the gateway target address (e.g. localhost:5001).
     * @return the open connection.
     */
    public static RemoteUdfGatewayConnection open(String udfGatewayTarget) {
        Preconditions.checkArgument(!udfGatewayTarget.isEmpty(), "Gateway target not configured!");

        ManagedChannel channel =
                Preconditions.checkNotNull(
                        ManagedChannelBuilder.forTarget(udfGatewayTarget).usePlaintext().build());

        SecureComputeGatewayGrpc.SecureComputeGatewayBlockingStub gateway =
                Preconditions.checkNotNull(SecureComputeGatewayGrpc.newBlockingStub(channel));

        return new RemoteUdfGatewayConnection(udfGatewayTarget, channel, gateway);
    }

    /** Closes the connection. */
    @Override
    public void close() {
        channel.shutdownNow();
    }

    public SecureComputeGatewayGrpc.SecureComputeGatewayBlockingStub getUdfGateway() {
        return udfGateway;
    }

    @Override
    public String toString() {
        return "RemoteUdfGatewayConnection{" + "udfGatewayTarget='" + udfGatewayTarget + '\'' + '}';
    }
}
