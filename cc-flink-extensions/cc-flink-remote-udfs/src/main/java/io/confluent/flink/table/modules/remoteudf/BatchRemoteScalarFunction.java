/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCacheImpl;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.udf.adapter.api.RemoteUdfSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_BATCH_SIZE;
import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_BATCH_WAIT_TIME_MS;

/** Batch version of {@link RemoteScalarFunction}, which can issue requests in batches. */
public class BatchRemoteScalarFunction extends AsyncScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(BatchRemoteScalarFunction.class);

    // Since we technically queue some additional requests, well allow a factor times the batch size
    public static final int QUEUE_CAPACITY_BATCH_FACTOR = 2;

    /** Config map for the remote UDF runtime. */
    private final Configuration config;

    /** The specification of the remote UDF, e.g. class name, argument and return value types. */
    private final RemoteUdfSpec remoteUdfSpec;

    /** Clock used for timing. Can be mocked in testing */
    private final Clock clock;

    /** The maximum size of a batch. */
    private final int batchSize;

    /** The maximum time to wait for a batch to collect. */
    private final long batchWaitTimeMs;

    /** The queue of requests in the current batch. */
    private final ArrayBlockingQueue<OutstandingRequest> currentRequestsBatch;

    /** Runtime to invoke the remote function. */
    private transient RemoteUdfRuntime remoteUdfRuntime;

    /** Tracks events which occur with UDFs. */
    private transient RemoteUdfMetrics metrics;

    /** Executor for processing sends operations. */
    private transient ExecutorService timerExecutor;

    private BatchRemoteScalarFunction(
            Configuration config, RemoteUdfSpec remoteUdfSpec, Clock clock) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(remoteUdfSpec);
        this.config = config;
        this.remoteUdfSpec = remoteUdfSpec;
        this.clock = clock;
        this.currentRequestsBatch =
                new ArrayBlockingQueue<>(
                        config.get(CONFLUENT_REMOTE_UDF_BATCH_SIZE) * QUEUE_CAPACITY_BATCH_FACTOR);
        this.batchSize = config.get(CONFLUENT_REMOTE_UDF_BATCH_SIZE);
        this.batchWaitTimeMs = config.get(CONFLUENT_REMOTE_UDF_BATCH_WAIT_TIME_MS).toMillis();
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

        currentRequestsBatch.add(new OutstandingRequest(completableFuture, args));
    }

    private void loop() {
        while (true) {
            final List<OutstandingRequest> requests = new ArrayList<>();
            try {
                drainTo(requests);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            sendRequest(requests);
        }
    }

    /**
     * This method blocks until there is something in the queue and until it returns a full batch,
     * or until the batch wait time is up. Either way, it returns a non-empty list ready to send.
     */
    private void drainTo(List<OutstandingRequest> requests) throws InterruptedException {
        Deadline deadline = null;
        while (deadline == null || deadline.hasTimeLeft()) {
            OutstandingRequest req =
                    currentRequestsBatch.poll(
                            deadline == null ? 100 : deadline.timeLeft().toMillis(),
                            TimeUnit.MILLISECONDS);
            if (req == null) {
                continue;
            }
            if (deadline == null) {
                deadline = Deadline.fromNow(Duration.ofMillis(batchWaitTimeMs));
            }
            requests.add(req);
            // Add any other requests that don't require waiting.
            currentRequestsBatch.drainTo(requests, batchSize - requests.size());
            if (requests.size() >= batchSize) {
                return;
            }
        }
    }

    private void sendRequest(final List<OutstandingRequest> requests) {
        // If we mark this to false, it will allow a new timer while we send this request.
        long startMs = clock.millis();
        try {
            final List<Object[]> args =
                    requests.stream().map(or -> or.args).collect(Collectors.toList());
            metrics.invocation(args.size());
            List<Object> result = remoteUdfRuntime.callRemoteUdfBatch(args);
            metrics.invocationSuccess(args.size());
            Preconditions.checkState(requests.size() == result.size());
            for (int i = 0; i < requests.size(); i++) {
                OutstandingRequest or = requests.get(i);
                or.future.complete(result.get(i));
            }
        } catch (Throwable t) {
            LOG.error("Got an error while doing UDF call", t);
            metrics.invocationFailure(requests.size());
            for (OutstandingRequest or : requests) {
                or.future.completeExceptionally(t);
            }
        } finally {
            long completeMs = clock.millis();
            metrics.invocationMs(completeMs - startMs);
        }
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
        timerExecutor = Executors.newSingleThreadExecutor();
        timerExecutor.execute(this::loop);
    }

    @Override
    public void close() throws Exception {
        if (remoteUdfRuntime != null) {
            metrics.instanceDeprovision();
            long startMs = clock.millis();
            try {
                this.remoteUdfRuntime.close();
            } catch (Throwable t) {
                LOG.error("Error closing runtime", t);
            }
            metrics.deprovisionMs(clock.millis() - startMs);
        }
        try {
            timerExecutor.shutdownNow();
        } catch (Throwable t) {
            LOG.error("Error shutting down executor", t);
        }
        super.close();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return UdfUtil.getTypeInference(
                remoteUdfSpec.getArgumentTypes(), remoteUdfSpec.getReturnType());
    }

    public static BatchRemoteScalarFunction create(
            Configuration config, RemoteUdfSpec remoteUdfSpec) {
        return new BatchRemoteScalarFunction(config, remoteUdfSpec, Clock.systemUTC());
    }

    @Override
    public boolean canReduceExpression() {
        return false;
    }

    @Override
    public boolean isDeterministic() {
        return remoteUdfSpec.isDeterministic();
    }

    /** Keeps track of outstanding requests. */
    private static class OutstandingRequest {
        private final CompletableFuture<Object> future;
        private final Object[] args;

        public OutstandingRequest(CompletableFuture<Object> future, Object[] args) {
            this.future = future;
            this.args = args;
        }

        public CompletableFuture<Object> getFuture() {
            return future;
        }

        public Object[] getArgs() {
            return args;
        }
    }
}
