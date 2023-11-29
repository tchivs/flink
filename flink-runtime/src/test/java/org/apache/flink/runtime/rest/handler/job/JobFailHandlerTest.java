/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.RestMatchers;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobFailRequestBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link JobFailHandler}. */
@Confluent
public class JobFailHandlerTest extends TestLogger {

    @Test
    public void testSuccessfulFailing() throws Exception {
        testResponse(jobId -> CompletableFuture.completedFuture(null), CompletableFuture::get);
    }

    @Test
    public void testErrorCodeForTimeout() throws Exception {
        testResponseRestCode(
                jobId -> FutureUtils.completedExceptionally(new TimeoutException()),
                HttpResponseStatus.REQUEST_TIMEOUT);
    }

    @Test
    public void testErrorCodeForUnknownJob() throws Exception {
        testResponseRestCode(
                jobId -> FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId)),
                HttpResponseStatus.NOT_FOUND);
    }

    @Test
    public void testErrorCodeForRandomError() throws Exception {
        testResponseRestCode(
                jobId ->
                        FutureUtils.completedExceptionally(new RuntimeException("Something wrong")),
                HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    private static void testResponseRestCode(
            Function<JobID, CompletableFuture<Void>> failJobFunction,
            HttpResponseStatus expectedErrorCode)
            throws Exception {

        testResponse(
                failJobFunction,
                failingFuture ->
                        assertThat(
                                failingFuture, RestMatchers.respondsWithError(expectedErrorCode)));
    }

    private static void testResponse(
            Function<JobID, CompletableFuture<Void>> failJobFunction,
            ThrowingConsumer<CompletableFuture<EmptyResponseBody>, Exception> assertion)
            throws Exception {
        final RestfulGateway gateway = createGateway(failJobFunction);

        try (JobFailHandler jobFailHandler = createHandler(gateway)) {
            JobMessageParameters messageParameters =
                    new JobMessageParameters().resolveJobId(new JobID());
            final CompletableFuture<EmptyResponseBody> failFuture =
                    jobFailHandler.handleRequest(
                            HandlerRequest.create(
                                    new JobFailRequestBody("Test reason"), messageParameters),
                            gateway);

            assertion.accept(failFuture);
        }
    }

    private static RestfulGateway createGateway(
            Function<JobID, CompletableFuture<Void>> failJobFunction) {
        return new TestingRestfulGateway.Builder().setFailJobFunction(failJobFunction).build();
    }

    private static JobFailHandler createHandler(RestfulGateway gateway) {
        return new JobFailHandler(
                () -> CompletableFuture.completedFuture(gateway),
                Time.hours(1),
                Collections.emptyMap());
    }
}
