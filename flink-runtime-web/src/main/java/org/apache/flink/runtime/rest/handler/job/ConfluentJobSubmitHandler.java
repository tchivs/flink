/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerConfluentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.job.ConfluentJobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.ConfluentJobSubmitRequestBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

/** This handler can be used to submit jobs to a Flink cluster. */
@Confluent
public final class ConfluentJobSubmitHandler
        extends AbstractRestHandler<
                DispatcherGateway,
                ConfluentJobSubmitRequestBody,
                EmptyResponseBody,
                EmptyMessageParameters> {

    static final Logger LOG = LoggerFactory.getLogger(ConfluentJobSubmitHandler.class);

    private final Configuration configuration;
    private final Executor executor;
    private final JobGraphGenerator jobGraphGenerator;
    private final Function<Throwable, RestHandlerException> exceptionClassifier;

    private final Semaphore inProgressRequests;

    private final ClassLoader classLoader = new GcPreventingURLCLassLoader(new URL[] {});

    public ConfluentJobSubmitHandler(
            GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            Time timeout,
            Map<String, String> headers,
            Configuration configuration,
            Executor executor) {
        this(
                leaderRetriever,
                timeout,
                headers,
                configuration,
                executor,
                ConfluentGeneratorUtils::generateJobGraph,
                ConfluentGeneratorUtils::convertJobGraphGenerationException);
    }

    @VisibleForTesting
    ConfluentJobSubmitHandler(
            GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            Time timeout,
            Map<String, String> headers,
            Configuration configuration,
            Executor executor,
            JobGraphGenerator jobGraphGenerator,
            Function<Throwable, RestHandlerException> exceptionClassifier) {
        super(leaderRetriever, timeout, headers, ConfluentJobSubmitHeaders.getInstance());
        this.configuration = configuration;
        this.executor = executor;
        this.jobGraphGenerator = jobGraphGenerator;
        this.exceptionClassifier = exceptionClassifier;

        final int configuredParallelism =
                configuration.get(JobManagerConfluentOptions.REST_MAX_PARALLEL_JOB_SUBMISSIONS);
        final int effectiveMaxParallelism = configuration.get(RestOptions.SERVER_NUM_THREADS);

        if (effectiveMaxParallelism < configuredParallelism) {
            LOG.warn(
                    "{} job submissions are meant to be processable in parallel, but the REST API only has {} threads. Picking the minimum value.",
                    configuredParallelism,
                    effectiveMaxParallelism);
        }

        this.inProgressRequests =
                new Semaphore(Math.min(configuredParallelism, effectiveMaxParallelism));

        try {
            generateJobGraph(
                    Collections.singleton(ResourceUtils.loadResource("/preload_plan.json")),
                    Collections.emptyMap());
        } catch (Throwable e) {
            LOG.warn("Preloading failed with an exception.", e);
        }
    }

    @Override
    protected CompletableFuture<EmptyResponseBody> handleRequest(
            @Nonnull HandlerRequest<ConfluentJobSubmitRequestBody> request,
            @Nonnull DispatcherGateway gateway) {

        if (!inProgressRequests.tryAcquire()) {
            return FutureUtils.completedExceptionally(
                    new ShortMultiRestHandlerException(
                            HttpResponseStatus.TOO_MANY_REQUESTS,
                            Collections.singletonList(
                                    "Too many job submissions already in-progress.")));
        }

        return CompletableFuture.supplyAsync(
                        () -> internalHandleRequest(request, gateway), executor)
                .thenCompose(Function.identity())
                .whenComplete((i1, i2) -> inProgressRequests.release());
    }

    private CompletableFuture<EmptyResponseBody> internalHandleRequest(
            @Nonnull HandlerRequest<ConfluentJobSubmitRequestBody> request,
            @Nonnull DispatcherGateway gateway) {

        log.info("Received job submission request.");
        final ConfluentJobSubmitRequestBody requestBody = request.getRequestBody();

        final Optional<RestHandlerException> restHandlerException = validateInput(requestBody);
        if (restHandlerException.isPresent()) {
            return FutureUtils.completedExceptionally(restHandlerException.get());
        }

        final Map<String, String> context =
                Collections.singletonMap(MdcUtils.JOB_ID, requestBody.jobId);
        try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(context)) {
            log.info("Validated job submission request.");

            final JobGraph jobGraph = processJobSubmission(requestBody);
            log.info("Generated job graph.");

            return gateway.submitJob(jobGraph, timeout)
                    .thenApply(
                            i -> {
                                try (MdcUtils.MdcCloseable ignored2 =
                                        MdcUtils.withContext(context)) {
                                    log.info("Submitted job.");
                                }
                                return EmptyResponseBody.getInstance();
                            });
        }
    }

    private static Optional<RestHandlerException> validateInput(
            ConfluentJobSubmitRequestBody requestBody) {
        final LinkedList<String> errors = new LinkedList<>();
        if (requestBody.jobId == null) {
            errors.add(ConfluentJobSubmitRequestBody.FIELD_NAME_JOB_ID + " must not be null.");
        } else {
            try {
                JobID.fromHexString(requestBody.jobId);
            } catch (Exception e) {
                errors.add(
                        ConfluentJobSubmitRequestBody.FIELD_NAME_JOB_ID
                                + " is not a valid job ID.");
            }
        }
        if (requestBody.generatorArguments.isEmpty()) {
            errors.add(
                    ConfluentJobSubmitRequestBody.FIELD_NAME_JOB_ARGUMENTS + " must not be empty.");
        }
        if (!errors.isEmpty()) {
            errors.addFirst("Input validation error.");
            return Optional.of(
                    new ShortMultiRestHandlerException(HttpResponseStatus.BAD_REQUEST, errors));
        }
        return Optional.empty();
    }

    private static class ShortMultiRestHandlerException extends RestHandlerException {

        private final List<String> errors;

        public ShortMultiRestHandlerException(
                HttpResponseStatus httpResponseStatus, List<String> errors) {
            super(String.join("\n\t", errors), httpResponseStatus);
            this.errors = errors;
        }

        @Override
        public ErrorResponseBody toErrorResponseBody() {
            return new ErrorResponseBody(errors);
        }
    }

    private JobGraph processJobSubmission(ConfluentJobSubmitRequestBody requestBody) {
        try {
            // Merge per-job configuration from the metaInfo into the Flink configuration so that it
            // is available for job graph generation.
            final Map<String, String> generatorConfiguration = configuration.toMap();
            generatorConfiguration.putAll(requestBody.configuration);

            JobGraph jobGraph =
                    generateJobGraph(requestBody.generatorArguments, generatorConfiguration);

            adjustJobGraph(
                    jobGraph,
                    JobID.fromHexString(requestBody.jobId),
                    requestBody.configuration,
                    requestBody.savepointPath);

            return jobGraph;
        } catch (Throwable e) {
            RestHandlerException finalException;
            try {
                finalException = exceptionClassifier.apply(e);
            } catch (Throwable t) {
                LOG.error("Internal error occurred while handling job graph generation error.", t);
                finalException =
                        new RestHandlerException(
                                "Internal error occurred.",
                                HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
            throw new CompletionException(finalException);
        }
    }

    private JobGraph generateJobGraph(
            Collection<String> arguments, Map<String, String> generatorConfiguration)
            throws Exception {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {
            return jobGraphGenerator.generateJobGraph(
                    arguments.iterator().next(), generatorConfiguration);
        }
    }

    private static void adjustJobGraph(
            JobGraph jobGraph,
            JobID jobId,
            Map<String, String> jobConfiguration,
            @Nullable String savepointPath) {
        jobGraph.setJobID(jobId);
        jobGraph.getJobConfiguration().addAll(Configuration.fromMap(jobConfiguration));
        if (savepointPath != null) {
            jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));
        }
    }

    interface JobGraphGenerator {
        JobGraph generateJobGraph(String compiledPlan, Map<String, String> configuration)
                throws Exception;
    }
}
