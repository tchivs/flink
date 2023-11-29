/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobFailHeaders;
import org.apache.flink.runtime.rest.messages.job.JobFailRequestBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/** Handler for failing the job. */
@Confluent
public class JobFailHandler
        extends AbstractRestHandler<
                RestfulGateway, JobFailRequestBody, EmptyResponseBody, JobMessageParameters> {
    public JobFailHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders) {
        super(leaderRetriever, timeout, responseHeaders, JobFailHeaders.getInstance());
    }

    @Override
    protected CompletableFuture<EmptyResponseBody> handleRequest(
            @Nonnull HandlerRequest<JobFailRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        JobFailRequestBody requestBody = request.getRequestBody();

        return gateway.failJob(
                        jobId, new ConfluentFailException(requestBody.getFailReason()), timeout)
                .handle(this::convertResult);
    }

    private EmptyResponseBody convertResult(Void ignore, Throwable throwable) {
        if (throwable == null) {
            return EmptyResponseBody.getInstance();
        }

        Throwable error = ExceptionUtils.stripCompletionException(throwable);
        if (error instanceof TimeoutException) {
            throw new CompletionException(
                    new RestHandlerException(
                            "Job failing timed out.", HttpResponseStatus.REQUEST_TIMEOUT, error));
        } else if (error instanceof FlinkJobNotFoundException) {
            throw new CompletionException(
                    new RestHandlerException(
                            "Job could not be found.", HttpResponseStatus.NOT_FOUND, error));
        } else {
            throw new CompletionException(
                    new RestHandlerException(
                            "Job failing failed: " + error.getMessage(),
                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            error));
        }
    }
}
