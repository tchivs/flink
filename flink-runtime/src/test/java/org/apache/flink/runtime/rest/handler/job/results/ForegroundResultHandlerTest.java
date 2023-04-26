/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job.results;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.OperatorIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.results.ForegroundResultHeaders;
import org.apache.flink.runtime.rest.messages.job.results.ForegroundResultRequestBody;
import org.apache.flink.runtime.rest.messages.job.results.ForegroundResultResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.TriFunction;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ForegroundResultHandler}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class ForegroundResultHandlerTest {

    private static final JobID jobId = JobID.generate();

    @Nested
    class InitialRequest {

        static final String NEW_VERSION = "new-version";

        @Test
        void testJobNotStarted() {
            assertThatThrownBy(
                            () ->
                                    testResponse(
                                            "",
                                            0L,
                                            jobStatusError(),
                                            coordinationResponseError(),
                                            jobResultError()))
                    .hasMessageContaining("Unable to fetch job results");
        }

        @Test
        void testJobStartingUp() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            "",
                            0L,
                            jobStatus(JobStatus.CREATED),
                            coordinationResponseError(),
                            jobResultError());
            assertThat(response.getVersion()).isEqualTo("");
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(0L);
            assertThat(response.getData()).isEqualTo("[]");
            assertThat(response.getRowCount()).isEqualTo(0);
        }

        @Test
        void testCoordinatorReachableWithoutData() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            "",
                            0L,
                            jobStatus(JobStatus.RUNNING),
                            coordinationResponse(NEW_VERSION, 0L),
                            jobResultError());
            assertThat(response.getVersion()).isEqualTo(NEW_VERSION);
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(0L);
            assertThat(response.getData()).isEqualTo("[]");
        }

        @Test
        void testCoordinationReachableWithData() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            "",
                            0L,
                            jobStatus(JobStatus.RUNNING),
                            coordinationResponse(NEW_VERSION, 0L, "A", "B", "C"),
                            jobResultError());
            assertThat(response.getVersion()).isEqualTo(NEW_VERSION);
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(0L);
            assertThat(response.getData()).isEqualTo("[A,B,C]");
            assertThat(response.getRowCount()).isEqualTo(3);
        }

        @Test
        void testJobTerminatedUngracefully() {
            assertThatThrownBy(
                            () ->
                                    testResponse(
                                            "",
                                            0L,
                                            jobStatus(JobStatus.CANCELED),
                                            coordinationResponseError(),
                                            jobResultError()))
                    .hasMessageContaining("Unable to fetch job results");
        }

        @Test
        void testJobTerminatedGracefullyWithData() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            "",
                            0L,
                            jobStatus(JobStatus.FINISHED),
                            coordinationResponseError(),
                            jobResult(0L, NEW_VERSION, 0L, "A", "B", "C"));
            assertThat(response.getVersion()).isEqualTo(NEW_VERSION);
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(0L);
            assertThat(response.getData()).isEqualTo("[A,B,C]");
        }

        @Test
        void testJobTerminatedGracefullyWithoutData() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            "",
                            0L,
                            jobStatus(JobStatus.FINISHED),
                            coordinationResponseError(),
                            jobResult(0L, NEW_VERSION, 0L));
            assertThat(response.getVersion()).isEqualTo(NEW_VERSION);
            // indicates completion by returning -1
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(-1L);
            assertThat(response.getData()).isEqualTo("[]");
        }
    }

    @Nested
    class IntermediateRequest {

        static final String OLD_VERSION = "some-version";

        @Test
        void testCoordinatorReachableWithoutData() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            OLD_VERSION,
                            2L,
                            jobStatus(JobStatus.RUNNING),
                            coordinationResponse(OLD_VERSION, 1L),
                            jobResultError());
            assertThat(response.getVersion()).isEqualTo(OLD_VERSION);
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(1L);
            assertThat(response.getData()).isEqualTo("[]");
        }

        @Test
        void testCoordinationReachableWithData() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            OLD_VERSION,
                            2L,
                            jobStatus(JobStatus.RUNNING),
                            coordinationResponse(OLD_VERSION, 1L, "C", "D"),
                            jobResultError());
            assertThat(response.getVersion()).isEqualTo(OLD_VERSION);
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(1L);
            assertThat(response.getData()).isEqualTo("[C,D]");
        }

        @Test
        void testJobTerminatedGracefullyWithData() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            OLD_VERSION,
                            2L,
                            jobStatus(JobStatus.FINISHED),
                            coordinationResponseError(),
                            jobResult(2L, OLD_VERSION, 1L, "C", "D"));
            assertThat(response.getVersion()).isEqualTo(OLD_VERSION);
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(1L);
            assertThat(response.getData()).isEqualTo("[C,D]");
        }

        @Test
        void testJobTerminatedGracefullyWithDataSecondRequest() throws Exception {
            final ForegroundResultResponseBody response =
                    testResponse(
                            OLD_VERSION,
                            4L, // now the request is more advanced than the static job result
                            jobStatus(JobStatus.FINISHED),
                            coordinationResponseError(),
                            jobResult(2L, OLD_VERSION, 1L, "C", "D"));
            assertThat(response.getVersion()).isEqualTo(OLD_VERSION);
            // indicates completion by returning -1
            assertThat(response.getLastCheckpointedOffset()).isEqualTo(-1L);
            assertThat(response.getData()).isEqualTo("[]");
        }

        @Test
        void testJobTerminatedUngracefully() {
            assertThatThrownBy(
                            () ->
                                    testResponse(
                                            OLD_VERSION,
                                            1L,
                                            jobStatus(JobStatus.CANCELED),
                                            coordinationResponseError(),
                                            jobResultError()))
                    .hasMessageContaining("Unable to fetch job results");
        }
    }

    // --------------------------------------------------------------------------------------------
    // Test utilities
    // --------------------------------------------------------------------------------------------

    public static ForegroundResultResponseBody testResponse(
            String requestVersion,
            long requestOffset,
            JobStatusFunction jobStatus,
            CoordinationResponseFunction coordinationResponse,
            JobResultFunction jobResult)
            throws Exception {
        final RestfulGateway gateway = createGateway(coordinationResponse, jobStatus, jobResult);

        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(JobIDPathParameter.KEY, jobId.toHexString());
        pathParameters.put(OperatorIDPathParameter.KEY, "7df19f87deec5680128845fd9a6ca18d");

        try (final ForegroundResultHandler handler =
                createHandler(gateway, new TestForegroundResultCoordinationAdapter())) {

            return handler.handleRequest(
                            HandlerRequest.resolveParametersAndCreate(
                                    new ForegroundResultRequestBody(requestVersion, requestOffset),
                                    handler.getMessageHeaders().getUnresolvedMessageParameters(),
                                    pathParameters,
                                    Collections.emptyMap(),
                                    Collections.emptyList()),
                            gateway)
                    .get();
        }
    }

    private static CoordinationResponseFunction coordinationResponseError() {
        return (jobId, operatorId, request) ->
                FutureUtils.completedExceptionally(
                        new RuntimeException("coordination response unavailable"));
    }

    private static CoordinationResponseFunction coordinationResponse(
            String version, long lastCheckpointedOffset, String... rows) {
        final TestCoordinationResponse coordinationResponse =
                new TestCoordinationResponse(version, lastCheckpointedOffset, Arrays.asList(rows));
        return (jobId, operatorId, request) ->
                CompletableFuture.completedFuture(coordinationResponse);
    }

    private static JobStatusFunction jobStatusError() {
        return (jobId) ->
                FutureUtils.completedExceptionally(new RuntimeException("job status unavailable"));
    }

    private static JobStatusFunction jobStatus(JobStatus jobStatus) {
        return (jobId) -> CompletableFuture.completedFuture(jobStatus);
    }

    private static JobResultFunction jobResultError() {
        return (jobId) ->
                FutureUtils.completedExceptionally(new RuntimeException("job result unavailable"));
    }

    private static JobResultFunction jobResult(
            long offset, String version, long lastCheckpointedOffset, String... rows) {
        final SerializedValue<OptionalFailure<Object>> serializedAcc;
        try {
            final byte[] acc =
                    InstantiationUtil.serializeObject(
                            new Tuple2<>(
                                    offset,
                                    new TestCoordinationResponse(
                                            version, lastCheckpointedOffset, Arrays.asList(rows))));
            final SerializedListAccumulator<byte[]> listAcc = new SerializedListAccumulator<>();
            listAcc.add(acc, BytePrimitiveArraySerializer.INSTANCE);
            serializedAcc = new SerializedValue<>(OptionalFailure.of(listAcc.getLocalValue()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final JobResult jobResult =
                new JobResult.Builder()
                        .jobId(jobId)
                        .applicationStatus(ApplicationStatus.SUCCEEDED)
                        .accumulatorResults(
                                Collections.singletonMap("cc-foreground-sink", serializedAcc))
                        .netRuntime(0)
                        .build();

        return (jobId) -> CompletableFuture.completedFuture(jobResult);
    }

    private static RestfulGateway createGateway(
            CoordinationResponseFunction toCoordinatorFunction,
            JobStatusFunction jobStatusFunction,
            JobResultFunction jobResultFunction) {
        return new TestingRestfulGateway.Builder()
                .setDeliverCoordinationRequestToCoordinatorFunction(toCoordinatorFunction)
                .setRequestJobStatusFunction(jobStatusFunction)
                .setRequestJobResultFunction(jobResultFunction)
                .build();
    }

    private static ForegroundResultHandler createHandler(
            RestfulGateway gateway, ForegroundResultCoordinationAdapter coordinationAdapter) {
        return new ForegroundResultHandler(
                () -> CompletableFuture.completedFuture(gateway),
                Time.hours(1),
                Collections.emptyMap(),
                ForegroundResultHeaders.getInstance(),
                coordinationAdapter);
    }

    private static class TestCoordinationRequest implements CoordinationRequest {
        String version;
        long offset;

        TestCoordinationRequest(String version, long offset) {
            this.version = version;
            this.offset = offset;
        }
    }

    private static class TestCoordinationResponse implements CoordinationResponse {
        String version;
        long lastCheckpointedOffset;
        List<String> rows;

        TestCoordinationResponse(String version, long lastCheckpointedOffset, List<String> rows) {
            this.version = version;
            this.lastCheckpointedOffset = lastCheckpointedOffset;
            this.rows = rows;
        }
    }

    private static class TestForegroundResultCoordinationAdapter
            implements ForegroundResultCoordinationAdapter {

        @Override
        public CoordinationRequest createRequest(String version, long offset) {
            return new TestCoordinationRequest(version, offset);
        }

        @Override
        public IntermediateResponse extractIntermediateResponse(CoordinationResponse response) {
            final TestCoordinationResponse testResponse = (TestCoordinationResponse) response;
            return new IntermediateResponse(
                    testResponse.version,
                    testResponse.lastCheckpointedOffset,
                    testResponse.rows.stream()
                            .map(r -> r.getBytes(StandardCharsets.UTF_8))
                            .collect(Collectors.toList()));
        }

        @Override
        public FinalResponse extractFinalResponse(byte[] acc) throws IOException {
            try {
                final Tuple2<Long, TestCoordinationResponse> testResponse =
                        InstantiationUtil.deserializeObject(
                                acc, ForegroundResultHandlerTest.class.getClassLoader());
                return new FinalResponse(
                        testResponse.f1.version,
                        testResponse.f1.lastCheckpointedOffset,
                        testResponse.f1.rows.stream()
                                .map(r -> r.getBytes(StandardCharsets.UTF_8))
                                .collect(Collectors.toList()),
                        testResponse.f0);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    interface CoordinationResponseFunction
            extends TriFunction<
                    JobID,
                    OperatorID,
                    SerializedValue<CoordinationRequest>,
                    CompletableFuture<CoordinationResponse>> {}

    interface JobStatusFunction extends Function<JobID, CompletableFuture<JobStatus>> {}

    interface JobResultFunction extends Function<JobID, CompletableFuture<JobResult>> {}
}
