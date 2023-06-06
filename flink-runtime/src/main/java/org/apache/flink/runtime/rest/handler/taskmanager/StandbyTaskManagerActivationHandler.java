/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.taskmanager.StandbyTaskManagerActivationHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.StandbyTaskManagerActivationRequestBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Rest handler which force the JobManager to activate standby TaskManager on given address. */
@Confluent
public class StandbyTaskManagerActivationHandler
        extends AbstractRestHandler<
                RestfulGateway,
                StandbyTaskManagerActivationRequestBody,
                EmptyResponseBody,
                EmptyMessageParameters> {

    public StandbyTaskManagerActivationHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                StandbyTaskManagerActivationHeaders.getInstance());
    }

    @Override
    protected CompletableFuture<EmptyResponseBody> handleRequest(
            @Nonnull HandlerRequest<StandbyTaskManagerActivationRequestBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        String rpcHost = request.getRequestBody().getRpcHost();
        String rpcPort = request.getRequestBody().getRpcPort();

        return gateway.activateStandbyTaskManager(rpcHost, rpcPort)
                .thenApply(ignored -> EmptyResponseBody.getInstance());
    }
}
