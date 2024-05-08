/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCacheImpl;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.confluent.flink.udf.adapter.api.RemoteUdfSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.shaded.guava31.com.google.common.util.concurrent.MoreExecutors.directExecutor;

/** Async version of {@link RemoteScalarFunction}, which can issue many requests at a time. */
public class AsyncRemoteScalarFunction extends AsyncScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteScalarFunction.class);

    /** Config map for the remote UDF runtime. */
    private final Configuration config;

    /** The specification of the remote UDF, e.g. class name, argument and return value types. */
    private final RemoteUdfSpec remoteUdfSpec;

    /** Runtime to invoke the remote function. */
    private transient RemoteUdfRuntime remoteUdfRuntime;

    /** Tracks events which occur with UDFs. */
    private transient RemoteUdfMetrics metrics;

    /** Executor for processing async operations. */
    private transient ExecutorService executor;

    /** Clock used for timing. Can be mocked in testing */
    private Clock clock;

    private AsyncRemoteScalarFunction(
            Configuration config, RemoteUdfSpec remoteUdfSpec, Clock clock) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(remoteUdfSpec);
        this.config = config;
        this.remoteUdfSpec = remoteUdfSpec;
        this.clock = clock;
    }

    /**
     * Calls the given remote function of given return type with the given payload and returns the
     * return value.
     *
     * @param args arguments for the remote function call.
     */
    public void eval(CompletableFuture<Object> completableFuture, Object... args) throws Exception {
        LOG.debug(
                "Invoking async remote scalar function. Plugin: {}, Function: {}, Rtype: {}, Args: {}",
                remoteUdfSpec.getPluginId(),
                remoteUdfSpec.getFunctionClassName(),
                remoteUdfSpec.getReturnType(),
                args);

        long startMs = clock.millis();
        metrics.invocation();
        com.google.common.util.concurrent.ListenableFuture<Object> future =
                remoteUdfRuntime.callRemoteUdfAsync(args, executor);
        com.google.common.util.concurrent.Futures.addCallback(
                future,
                new com.google.common.util.concurrent.FutureCallback<Object>() {
                    @Override
                    public void onSuccess(Object result) {
                        metrics.invocationSuccess();
                        complete();
                        completableFuture.complete(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        LOG.error("Got an error while doing UDF call", t);
                        metrics.invocationFailure();
                        complete();
                        completableFuture.completeExceptionally(t);
                    }

                    private void complete() {
                        long completeMs = clock.millis();
                        metrics.invocationMs(completeMs - startMs);
                    }
                },
                directExecutor());
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        this.metrics = new RemoteUdfMetrics(context.getMetricGroup());

        // Open should never be called twice, but just in case...
        if (this.remoteUdfRuntime != null) {
            remoteUdfRuntime.close();
        }

        metrics.instanceProvision();
        Preconditions.checkNotNull(remoteUdfSpec);
        this.remoteUdfRuntime =
                RemoteUdfRuntime.open(
                        config,
                        remoteUdfSpec,
                        metrics,
                        KafkaCredentialsCacheImpl.INSTANCE,
                        context.getJobId());
        // Shouldn't be any larger than the maximum number of outstanding requests.
        executor =
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder()
                                .setNameFormat(
                                        String.format("async-rsf-%s", context.getJobId()) + "-%d")
                                .build());
    }

    @Override
    public void close() throws Exception {
        if (remoteUdfRuntime != null) {
            metrics.instanceDeprovision();
            this.remoteUdfRuntime.close();
        }
        executor.shutdownNow();
        super.close();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return UdfUtil.getTypeInference(
                remoteUdfSpec.getArgumentTypes(), remoteUdfSpec.getReturnType());
    }

    public static AsyncRemoteScalarFunction create(
            Configuration config, RemoteUdfSpec remoteUdfSpec) {
        LOG.info("AsyncRemoteScalarFunction config: {}", config);
        return new AsyncRemoteScalarFunction(config, remoteUdfSpec, Clock.systemUTC());
    }

    @Override
    public boolean canReduceExpression() {
        return false;
    }

    @Override
    public boolean isDeterministic() {
        return remoteUdfSpec.isDeterministic();
    }
}
