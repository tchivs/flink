/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.util.Preconditions;

import io.confluent.flink.apiserver.client.model.ComputeV1alphaFlinkUdfTask;
import io.confluent.secure.compute.gateway.v1.SecureComputeGatewayGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import javax.net.ssl.SSLException;

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
     * @param udfTask containing the gateway target address (e.g. localhost:5001) as part of its
     *     Status.
     * @return the open connection.
     */
    public static RemoteUdfGatewayConnection open(ComputeV1alphaFlinkUdfTask udfTask)
            throws SSLException {
        Preconditions.checkArgument(
                !udfTask.getStatus().getEndpoint().getHost().isEmpty(),
                "Gateway Host not configured!");
        Preconditions.checkArgument(
                !udfTask.getStatus().getEndpoint().getPort().equals(0),
                "Gateway Port not configured!");

        String udfGatewayTarget =
                String.format(
                        "%s:%d",
                        udfTask.getStatus().getEndpoint().getHost(),
                        udfTask.getStatus().getEndpoint().getPort());

        ManagedChannel channel =
                NettyChannelBuilder.forTarget(udfGatewayTarget)
                        .sslContext(
                                GrpcSslContexts.forClient()
                                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                                        .build())
                        .build();

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
