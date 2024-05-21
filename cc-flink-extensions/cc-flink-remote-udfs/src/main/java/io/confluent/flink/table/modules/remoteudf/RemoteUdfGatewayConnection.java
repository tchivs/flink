/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.apiserver.client.model.ComputeV1FlinkUdfTask;
import io.confluent.flink.table.modules.remoteudf.utils.CallWithRetry;
import io.confluent.secure.compute.gateway.v1.SecureComputeGatewayGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import javax.net.ssl.SSLException;

import java.io.Closeable;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.GATEWAY_RETRY_BACKOFF_MS;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.GATEWAY_RETRY_MAX_ATTEMPTS;

/** Encapsulates a gRPC connection to communicate with remote UDF service gateway. */
public class RemoteUdfGatewayConnection implements Closeable {
    /** Target address for the udfGateway. */
    private final String udfGatewayTarget;

    /** The managed channel used to construct the udfGateway. */
    private ManagedChannel channel;

    /** Gateway to invoke remote UDFs. */
    private SecureComputeGatewayGrpc.SecureComputeGatewayBlockingStub udfGateway;

    private final SecureComputeGatewayGrpc.SecureComputeGatewayFutureStub asyncGateway;
    /** Gateway to invoke remote UDFs with retry. */
    private final CallWithRetry callWithRetry;

    /** Deadline for the gateway service. */
    private final int deadlineSeconds;

    private RemoteUdfGatewayConnection(
            String udfGatewayTarget,
            ManagedChannel channel,
            SecureComputeGatewayGrpc.SecureComputeGatewayBlockingStub udfGateway,
            SecureComputeGatewayGrpc.SecureComputeGatewayFutureStub asyncGateway,
            CallWithRetry retryCall,
            int deadlineSeconds) {
        this.udfGatewayTarget = udfGatewayTarget;
        this.channel = channel;
        this.udfGateway = udfGateway;
        this.asyncGateway = asyncGateway;
        this.callWithRetry = retryCall;
        this.deadlineSeconds = deadlineSeconds;
    }

    /**
     * Opens a connection to the given target gateway.
     *
     * @param udfTask containing the gateway target address (e.g. localhost:5001) as part of its
     *     Status.
     * @param config containing the retry configuration.
     * @return the open connection.
     */
    public static RemoteUdfGatewayConnection open(
            ComputeV1FlinkUdfTask udfTask, Configuration config) throws SSLException {
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
        SecureComputeGatewayGrpc.SecureComputeGatewayFutureStub asyncGateway =
                Preconditions.checkNotNull(SecureComputeGatewayGrpc.newFutureStub(channel));

        CallWithRetry retryCall =
                new CallWithRetry(
                        config.getInteger(GATEWAY_RETRY_MAX_ATTEMPTS),
                        config.getLong(GATEWAY_RETRY_BACKOFF_MS));
        int deadlineSeconds = config.getInteger(RemoteUdfModule.GATEWAY_SERVICE_DEADLINE_SEC);

        return new RemoteUdfGatewayConnection(
                udfGatewayTarget, channel, gateway, asyncGateway, retryCall, deadlineSeconds);
    }

    /** Closes the connection. */
    @Override
    public void close() {
        channel.shutdownNow();
    }

    public SecureComputeGatewayGrpc.SecureComputeGatewayBlockingStub getUdfGateway() {
        return udfGateway;
    }

    public SecureComputeGatewayGrpc.SecureComputeGatewayFutureStub getAsyncUdfGateway() {
        return asyncGateway;
    }

    public CallWithRetry getCallWithRetry() {
        return callWithRetry;
    }

    public int getDeadlineSeconds() {
        return deadlineSeconds;
    }

    @Override
    public String toString() {
        return "RemoteUdfGatewayConnection{" + "udfGatewayTarget='" + udfGatewayTarget + '\'' + '}';
    }
}
