/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerConfluentOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.job.ConfluentJobSubmitRequestBody;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ConfluentJobSubmitHandler}. */
@Confluent
class ConfluentJobSubmitHandlerTest {

    private static final int maxParallelism = 12345;
    private static final Configuration clusterConfig =
            UnmodifiableConfiguration.fromMap(
                    Collections.singletonMap(
                            PipelineOptions.MAX_PARALLELISM.key(), String.valueOf(maxParallelism)));

    private static final String COMPILED_PLAN = loadCompiledPlan();

    @RegisterExtension
    static final TestExecutorExtension<ExecutorService> EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(Executors::newSingleThreadExecutor);

    @RegisterExtension
    final LoggerAuditingExtension loggerExtension =
            new LoggerAuditingExtension(ConfluentJobSubmitHandler.class, Level.INFO);

    /**
     * Verifies that the generated job graph uses a default parallelism of 1.
     *
     * <p>This test MUST NOT be run with a MiniClusterExtension because it affects the generated
     * graph.
     */
    @Test
    void testGeneratedJobGraphHasParallelismOf1() throws Exception {
        final HandlerRequest<ConfluentJobSubmitRequestBody> request =
                createRequest(
                        JobID.generate().toHexString(),
                        null,
                        Collections.singleton(loadCompiledPlan()),
                        Collections.emptyMap());

        final JobGraph jobGraph = submitAndRetrieveJobGraph(request);

        assertThat(jobGraph.getMaximumParallelism()).isEqualTo(1);
    }

    @Test
    void testSetJobID() throws Exception {
        final JobID jobId = JobID.generate();

        final HandlerRequest<ConfluentJobSubmitRequestBody> request =
                createRequest(
                        jobId.toHexString(),
                        null,
                        Collections.singleton(COMPILED_PLAN),
                        Collections.emptyMap());

        final JobGraph jobGraph = submitAndRetrieveJobGraph(request);

        assertThat(jobGraph.getJobID()).isEqualTo(jobId);
    }

    @Test
    void testSetJobName() throws Exception {
        final JobID jobId = JobID.generate();

        final HandlerRequest<ConfluentJobSubmitRequestBody> request =
                createRequest(
                        jobId.toHexString(),
                        null,
                        Collections.singleton(COMPILED_PLAN),
                        Collections.emptyMap());

        final JobGraph jobGraph = submitAndRetrieveJobGraph(request);

        assertThat(jobGraph.getName()).isEqualTo(jobId.toHexString());
    }

    @Test
    void testSetJobConfigurationOnJobGraph() throws Exception {
        final Map<String, String> jobConfiguration = Collections.singletonMap("foo", "bar");

        final HandlerRequest<ConfluentJobSubmitRequestBody> request =
                createRequest(
                        JobID.generate().toHexString(),
                        null,
                        Collections.singleton(loadCompiledPlan()),
                        jobConfiguration);

        final JobGraph jobGraph = submitAndRetrieveJobGraph(request);

        assertThat(jobGraph.getJobConfiguration().toMap()).containsAllEntriesOf(jobConfiguration);
        assertThat(
                        jobGraph.getSerializedExecutionConfig()
                                .deserializeValue(getClass().getClassLoader())
                                .getMaxParallelism())
                .isEqualTo(maxParallelism);
    }

    @Test
    void testUseClusterConfigurationDuringGeneration() throws Exception {
        final HandlerRequest<ConfluentJobSubmitRequestBody> request =
                createRequest(
                        JobID.generate().toHexString(),
                        null,
                        Collections.singleton(loadCompiledPlan()),
                        Collections.emptyMap());

        final JobGraph jobGraph = submitAndRetrieveJobGraph(request);

        assertThat(
                        jobGraph.getSerializedExecutionConfig()
                                .deserializeValue(getClass().getClassLoader())
                                .getMaxParallelism())
                .isEqualTo(maxParallelism);
    }

    @Test
    void testJobConfigMergedIntoClusterConfigurationDuringGeneration() throws Exception {
        final int customMaxParallelism = 22222;
        final Map<String, String> jobConfiguration =
                Collections.singletonMap(
                        PipelineOptions.MAX_PARALLELISM.key(),
                        String.valueOf(customMaxParallelism));

        final HandlerRequest<ConfluentJobSubmitRequestBody> request =
                createRequest(
                        JobID.generate().toHexString(),
                        null,
                        Collections.singleton(loadCompiledPlan()),
                        jobConfiguration);

        final JobGraph jobGraph = submitAndRetrieveJobGraph(request);

        assertThat(
                        jobGraph.getSerializedExecutionConfig()
                                .deserializeValue(getClass().getClassLoader())
                                .getMaxParallelism())
                .isEqualTo(customMaxParallelism);
    }

    @Test
    void testSetSavepointPath() throws Exception {
        final String savepoint = "savepoint";

        final HandlerRequest<ConfluentJobSubmitRequestBody> request =
                createRequest(
                        JobID.generate().toHexString(),
                        savepoint,
                        Collections.singleton(COMPILED_PLAN),
                        Collections.emptyMap());

        final JobGraph jobGraph = submitAndRetrieveJobGraph(request);

        assertThat(jobGraph.getSavepointRestoreSettings().getRestorePath()).isEqualTo(savepoint);
    }

    @Test
    void testLoggingOnSuccessfulSubmission() throws Exception {
        final String jobId = JobID.generate().toHexString();
        final HandlerRequest<ConfluentJobSubmitRequestBody> request =
                createRequest(
                        jobId,
                        null,
                        Collections.singleton(loadCompiledPlan()),
                        Collections.emptyMap());

        submitAndRetrieveJobGraph(request);

        assertThat(loggerExtension.getEvents())
                .anySatisfy(
                        event -> {
                            assertThat(event.getMessage().getFormattedMessage())
                                    .contains("Validated");
                            assertThat(event.getContextData().toMap())
                                    .containsEntry(MdcUtils.JOB_ID, jobId);
                        })
                .anySatisfy(
                        event -> {
                            assertThat(event.getMessage().getFormattedMessage())
                                    .contains("Generated");
                            assertThat(event.getContextData().toMap())
                                    .containsEntry(MdcUtils.JOB_ID, jobId);
                        })
                .anySatisfy(
                        event -> {
                            assertThat(event.getMessage().getFormattedMessage())
                                    .contains("Submitted");
                            assertThat(event.getContextData().toMap())
                                    .containsEntry(MdcUtils.JOB_ID, jobId);
                        });
    }

    @Test
    void testRateLimiting() throws Exception {
        final BlockerSync blockerSync = new BlockerSync();

        final DispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                i -> {
                                    blockerSync.blockNonInterruptible();
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        final int numParallelSubmissions =
                JobManagerConfluentOptions.REST_MAX_PARALLEL_JOB_SUBMISSIONS.defaultValue();

        try (ConfluentJobSubmitHandler handler = createHandler()) {
            // submit enough statements to exhaust parallelism budged
            List<CompletableFuture<EmptyResponseBody>> submissions =
                    IntStream.range(0, numParallelSubmissions)
                            .mapToObj(i -> submitRandomRequest(handler, dispatcherGateway))
                            .collect(Collectors.toList());

            // further requests should fail
            assertThatThrownBy(() -> submitRandomRequest(handler, dispatcherGateway).join())
                    .cause()
                    .satisfies(
                            exception -> {
                                assertThat(exception).isInstanceOf(RestHandlerException.class);
                                assertThat(
                                                ((RestHandlerException) exception)
                                                        .getHttpResponseStatus())
                                        .isEqualTo(HttpResponseStatus.TOO_MANY_REQUESTS);
                            });

            // unblock ongoing job submissions
            // none of these should fail
            blockerSync.releaseBlocker();
            FutureUtils.waitForAll(submissions).join();

            // new job submissions should go through
            submitRandomRequest(handler, dispatcherGateway).join();
        }

        assertThat(loggerExtension.getEvents())
                .noneSatisfy(
                        event -> {
                            assertThat(event.getMessage().getFormattedMessage())
                                    .contains("Too many job submissions");
                            assertThat(event.getContextData().toMap()).containsKey(MdcUtils.JOB_ID);
                        });
    }

    private static CompletableFuture<EmptyResponseBody> submitRandomRequest(
            ConfluentJobSubmitHandler handler, DispatcherGateway gateway) {
        return handler.handleRequest(
                createRequest(
                        JobID.generate().toHexString(),
                        null,
                        Collections.singleton(COMPILED_PLAN),
                        Collections.emptyMap()),
                gateway);
    }

    @Test
    void testParameterValidation() throws Exception {
        testErrorHandling(
                createRequest(
                        null, null, Collections.singleton(COMPILED_PLAN), Collections.emptyMap()),
                HttpResponseStatus.BAD_REQUEST,
                "jobId must not be null");

        testErrorHandling(
                createRequest(
                        "invalid-job-id",
                        null,
                        Collections.singleton(COMPILED_PLAN),
                        Collections.emptyMap()),
                HttpResponseStatus.BAD_REQUEST,
                "jobId is not a valid job ID");

        testErrorHandling(
                createRequest(
                        JobID.generate().toHexString(),
                        null,
                        Collections.emptyList(),
                        Collections.emptyMap()),
                HttpResponseStatus.BAD_REQUEST,
                "generatorArguments must not be empty");

        assertThat(loggerExtension.getEvents())
                .noneSatisfy(
                        event ->
                                assertThat(event.getContextData().toMap())
                                        .containsKey(MdcUtils.JOB_ID));
    }

    @Test
    void testJoGraphGenerationErrorHandling() throws Exception {
        final String compiledPlan = "foo bar baz";

        final Exception generationFailure = new RuntimeException("generation failure");
        final String jobId = JobID.generate().toHexString();

        testErrorHandling(
                createHandler(
                        (i1, i2, i3) -> {
                            ConfluentJobSubmitHandler.LOG.error("generation error");
                            throw generationFailure;
                        },
                        e ->
                                new RestHandlerException(
                                        e.getMessage(), HttpResponseStatus.BAD_REQUEST)),
                createRequest(
                        jobId, null, Collections.singleton(compiledPlan), Collections.emptyMap()),
                HttpResponseStatus.BAD_REQUEST,
                generationFailure.getMessage());

        assertThat(loggerExtension.getEvents())
                .anySatisfy(
                        event -> {
                            assertThat(event.getMessage().getFormattedMessage())
                                    .isEqualTo("generation error");
                            assertThat(event.getContextData().toMap())
                                    .containsEntry(MdcUtils.JOB_ID, jobId);
                        });
    }

    @Test
    void testJoGraphGenerationErrorClassificationErrorHandling() throws Exception {
        final String compiledPlan = "foo bar baz";

        final Exception generationFailure = new RuntimeException("generation failure");
        final RuntimeException classificationFailure =
                new RuntimeException("classification failure");
        final String jobId = JobID.generate().toHexString();

        testErrorHandling(
                createHandler(
                        (i1, i2, i3) -> {
                            throw generationFailure;
                        },
                        e -> {
                            throw classificationFailure;
                        }),
                createRequest(
                        jobId, null, Collections.singleton(compiledPlan), Collections.emptyMap()),
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Internal error occurred");

        assertThat(loggerExtension.getEvents())
                .anySatisfy(
                        event -> {
                            assertThat(event.getMessage().getFormattedMessage())
                                    .isEqualTo(
                                            "Internal error occurred while handling job graph generation error.");
                            assertThat(event.getContextData().toMap())
                                    .containsEntry(MdcUtils.JOB_ID, jobId);
                        });
    }

    private static void testErrorHandling(
            HandlerRequest<ConfluentJobSubmitRequestBody> request,
            HttpResponseStatus expectedError,
            String errorMessage)
            throws Exception {
        testErrorHandling(createHandler(), request, expectedError, errorMessage);
    }

    private static void testErrorHandling(
            ConfluentJobSubmitHandler handler,
            HandlerRequest<ConfluentJobSubmitRequestBody> request,
            HttpResponseStatus expectedError,
            String errorMessage)
            throws Exception {

        try {

            assertThatThrownBy(
                            () ->
                                    handler.handleRequest(
                                                    request,
                                                    TestingDispatcherGateway.newBuilder().build())
                                            .join())
                    .cause()
                    .satisfies(
                            exception -> {
                                assertThat(exception).isInstanceOf(RestHandlerException.class);
                                assertThat(exception).hasMessageContaining(errorMessage);
                                assertThat(
                                                ((RestHandlerException) exception)
                                                        .getHttpResponseStatus())
                                        .isEqualTo(expectedError);
                            });
        } finally {
            handler.close();
        }
    }

    private static HandlerRequest<ConfluentJobSubmitRequestBody> createRequest(
            @Nullable String jobId,
            @Nullable String savepointPath,
            @Nullable Collection<String> generatorArguments,
            @Nullable Map<String, String> jobConfiguration) {
        final ConfluentJobSubmitRequestBody request =
                new ConfluentJobSubmitRequestBody(
                        jobId, savepointPath, generatorArguments, jobConfiguration);
        return HandlerRequest.create(request, EmptyMessageParameters.getInstance());
    }

    private JobGraph submitAndRetrieveJobGraph(
            HandlerRequest<ConfluentJobSubmitRequestBody> request) throws Exception {

        final CompletableFuture<JobGraph> jobGraphFuture = new CompletableFuture<>();
        final DispatcherGateway gateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    jobGraphFuture.complete(jobGraph);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        try (ConfluentJobSubmitHandler handler = createHandler()) {
            handler.handleRequest(request, gateway).join();
            return jobGraphFuture.join();
        }
    }

    private static ConfluentJobSubmitHandler createHandler() {
        return new ConfluentJobSubmitHandler(
                CompletableFuture::new,
                Time.seconds(10),
                Collections.emptyMap(),
                clusterConfig,
                EXECUTOR_EXTENSION.getExecutor());
    }

    private static ConfluentJobSubmitHandler createHandler(
            ConfluentJobSubmitHandler.JobGraphGenerator jobGraphGenerator,
            Function<Throwable, RestHandlerException> exceptionClassifier) {
        return new ConfluentJobSubmitHandler(
                CompletableFuture::new,
                Time.seconds(10),
                Collections.emptyMap(),
                clusterConfig,
                EXECUTOR_EXTENSION.getExecutor(),
                jobGraphGenerator,
                exceptionClassifier);
    }

    static String loadCompiledPlan() {
        try {
            return ResourceUtils.loadResource("/compiled_plan.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
