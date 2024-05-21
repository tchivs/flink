/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job.results;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.results.ForegroundResultCoordinationAdapter.FinalResponse;
import org.apache.flink.runtime.rest.handler.job.results.ForegroundResultCoordinationAdapter.IntermediateResponse;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.OperatorIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.results.ForegroundResultMessageParameters;
import org.apache.flink.runtime.rest.messages.job.results.ForegroundResultRequestBody;
import org.apache.flink.runtime.rest.messages.job.results.ForegroundResultResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Handler that receives foreground result requests and returns the response from the coordinator or
 * accumulator.
 */
@Confluent
public class ForegroundResultHandler
        extends AbstractRestHandler<
                RestfulGateway,
                ForegroundResultRequestBody,
                ForegroundResultResponseBody,
                ForegroundResultMessageParameters> {

    private static final Logger LOG = LoggerFactory.getLogger(ForegroundResultHandler.class);

    /**
     * This adapter exists for not messing up our Flink fork too much. See {@link
     * ForegroundResultCoordinationAdapter} for more information.
     */
    private static final String COORDINATION_ADAPTER =
            "org.apache.flink.streaming.api.operators.collect.DefaultForegroundResultCoordinationAdapter";

    private static final String ACCUMULATOR_NAME = "cc-foreground-sink";

    private final ForegroundResultCoordinationAdapter coordinationAdapter;

    @VisibleForTesting
    public ForegroundResultHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            ForegroundResultRequestBody,
                            ForegroundResultResponseBody,
                            ForegroundResultMessageParameters>
                    messageHeaders,
            ForegroundResultCoordinationAdapter coordinationAdapter) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.coordinationAdapter = coordinationAdapter;
    }

    public ForegroundResultHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            ForegroundResultRequestBody,
                            ForegroundResultResponseBody,
                            ForegroundResultMessageParameters>
                    messageHeaders) {
        this(
                leaderRetriever,
                timeout,
                responseHeaders,
                messageHeaders,
                resolveCoordinationAdapter());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static @Nullable ForegroundResultCoordinationAdapter resolveCoordinationAdapter() {
        try {
            final Class<ForegroundResultCoordinationAdapter> coordinationAdapterClass =
                    (Class) Class.forName(COORDINATION_ADAPTER);
            return coordinationAdapterClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            LOG.warn("Unable to load coordination adapter: " + COORDINATION_ADAPTER, e);
            return null;
        }
    }

    @Override
    protected CompletableFuture<ForegroundResultResponseBody> handleRequest(
            @Nonnull HandlerRequest<ForegroundResultRequestBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        final OperatorID operatorId = request.getPathParameter(OperatorIDPathParameter.class);
        final ForegroundResultRequestBody requestBody = request.getRequestBody();

        if (coordinationAdapter == null) {
            throw new CompletionException(
                    new RestHandlerException(
                            "Failed to load coordination adapter for foreground results.",
                            HttpResponseStatus.INTERNAL_SERVER_ERROR));
        }

        final String requestVersion = requestBody.getVersion().orElse("");
        final long requestOffset = requestBody.getOffset().orElse(0L);

        final CoordinationRequest coordinationRequest =
                coordinationAdapter.createRequest(requestVersion, requestOffset);

        final SerializedValue<CoordinationRequest> serializedRequest;
        try {
            serializedRequest = new SerializedValue<>(coordinationRequest);
        } catch (IOException e) {
            throw new RestHandlerException(
                    "Failed to serialize coordination request",
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    e);
        }

        final CompletableFuture<CoordinationResponse> responseFuture =
                gateway.deliverCoordinationRequestToCoordinator(
                        jobId, operatorId, serializedRequest, timeout);

        return responseFuture
                .thenApply(
                        response -> {
                            // Happy path: Job is running and reachable
                            final IntermediateResponse intermediateResponse =
                                    coordinationAdapter.extractIntermediateResponse(response);
                            return ForegroundResultResponseBody.of(
                                    intermediateResponse.version,
                                    intermediateResponse.lastCheckpointedOffset,
                                    intermediateResponse.rows,
                                    false);
                        })
                .exceptionally(
                        t -> {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Job coordinator not reachable", t);
                            }

                            if (!isJobTerminated(gateway, jobId)) {
                                // Job is running but not reachable (yet)

                                // Communicate to retry with lastCheckpointOffset = -1 which
                                // should have no impact on user-visible buffer
                                return ForegroundResultResponseBody.of(
                                        requestVersion, -1L, Collections.emptyList(), false);
                            }

                            // Job terminated
                            final FinalResponse finalResponse;
                            try {
                                final CompletableFuture<JobResult> jobResultFuture =
                                        gateway.requestJobResult(jobId, timeout);
                                finalResponse = readAccumulator(jobResultFuture.get());
                            } catch (Exception e) {
                                // Job terminated ungracefully or never existed
                                throw new CompletionException(
                                        new RestHandlerException(
                                                "Unable to fetch job results.",
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                e));
                            }

                            // Job terminated gracefully
                            if (!finalResponse.rows.isEmpty()
                                    && requestOffset <= finalResponse.offset) {
                                // Serve remaining results
                                final int startFrom = (int) (finalResponse.offset - requestOffset);
                                final List<byte[]> remainingRows =
                                        finalResponse.rows.subList(
                                                startFrom, finalResponse.rows.size());
                                return ForegroundResultResponseBody.of(
                                        finalResponse.version,
                                        finalResponse.lastCheckpointedOffset,
                                        remainingRows,
                                        true);
                            }

                            // Temporary logging for debugging
                            LOG.info(
                                    "EOS result for job {}. "
                                            + "Request version: {}, "
                                            + "Request offset: {}, "
                                            + "Final response version: {}, "
                                            + "Final response offset: {}, "
                                            + "Final response rows: {}",
                                    jobId.toString(),
                                    requestVersion,
                                    requestOffset,
                                    finalResponse.version,
                                    finalResponse.offset,
                                    finalResponse.rows.size());

                            // End-of-stream (EOS)
                            // Communicate that job terminated with lastCheckpointOffset = -1 which
                            // should have no impact on user-visible buffer
                            return ForegroundResultResponseBody.of(
                                    finalResponse.version, -1L, Collections.emptyList(), true);
                        });
    }

    private boolean isJobTerminated(RestfulGateway gateway, JobID jobId) {
        try {
            final CompletableFuture<JobStatus> jobStatusFuture =
                    gateway.requestJobStatus(jobId, timeout);
            final JobStatus jobStatus = jobStatusFuture.get();
            return jobStatus.isGloballyTerminalState();
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unknown job terminal state", e);
            }
            // In case of any error, we assume that the job has been terminated.
            return true;
        }
    }

    @SuppressWarnings("unchecked")
    private FinalResponse readAccumulator(JobResult jobResult) throws Exception {
        if (!jobResult.isSuccess()) {
            throw new IOException("Job terminated ungracefully.");
        }
        final SerializedValue<OptionalFailure<Object>> serializedListAccWithFailure =
                jobResult.getAccumulatorResults().get(ACCUMULATOR_NAME);
        final OptionalFailure<Object> listAccWithFailure =
                serializedListAccWithFailure.deserializeValue(
                        Thread.currentThread().getContextClassLoader());
        final ArrayList<byte[]> wrappedListAcc =
                (ArrayList<byte[]>) listAccWithFailure.getUnchecked();
        final List<byte[]> listAcc =
                SerializedListAccumulator.deserializeList(
                        wrappedListAcc, BytePrimitiveArraySerializer.INSTANCE);
        final byte[] acc = listAcc.get(0);
        return coordinationAdapter.extractFinalResponse(acc);
    }
}
