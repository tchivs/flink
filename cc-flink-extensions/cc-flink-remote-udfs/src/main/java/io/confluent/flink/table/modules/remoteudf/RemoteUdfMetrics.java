/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;

import java.util.concurrent.atomic.AtomicLong;

/** All the metrics for remote Udfs. */
public class RemoteUdfMetrics {

    static final String METRIC_NAME = "ConfluentUdf";

    static final String INVOCATION_NAME = "invocations";
    static final String INVOCATION_SUCCESSES_NAME = "invocationSuccesses";
    static final String INVOCATION_FAILURES_NAME = "invocationFailures";
    static final String INVOCATION_MS_NAME = "invocationsMs";
    static final String PROVISIONS_NAME = "provisions";
    static final String DEPROVISIONS_NAME = "deprovisions";
    static final String BYTES_TO_UDF_NAME = "bytesToUdf";
    static final String BYTES_FROM_UDF_NAME = "bytesFromUdf";

    private final MetricGroup group;

    private final Counter invocations;
    private final Counter invocationFailures;
    private final Counter invocationSuccesses;
    private final Gauge<Long> invocationMs;
    private final Counter provisions;
    private final Counter deprovisions;
    private final Counter bytesToUdf;
    private final Counter bytesFromUdf;

    private final AtomicLong lastInvocationMs = new AtomicLong(0);

    public RemoteUdfMetrics(MetricGroup parentGroup) {
        this.group = parentGroup.addGroup(METRIC_NAME);
        this.invocations = group.counter(INVOCATION_NAME, new ThreadSafeSimpleCounter());
        this.invocationSuccesses =
                group.counter(INVOCATION_SUCCESSES_NAME, new ThreadSafeSimpleCounter());
        this.invocationFailures =
                group.counter(INVOCATION_FAILURES_NAME, new ThreadSafeSimpleCounter());
        this.invocationMs = group.gauge(INVOCATION_MS_NAME, lastInvocationMs::get);
        this.provisions = group.counter(PROVISIONS_NAME, new ThreadSafeSimpleCounter());
        this.deprovisions = group.counter(DEPROVISIONS_NAME, new ThreadSafeSimpleCounter());
        this.bytesToUdf = group.counter(BYTES_TO_UDF_NAME, new ThreadSafeSimpleCounter());
        this.bytesFromUdf = group.counter(BYTES_FROM_UDF_NAME, new ThreadSafeSimpleCounter());
    }

    public void invocation() {
        invocations.inc();
    }

    public void invocationMs(long invocationMs) {
        lastInvocationMs.set(invocationMs);
    }

    public void invocationSuccess() {
        invocationSuccesses.inc();
    }

    public void invocationFailure() {
        invocationFailures.inc();
    }

    public void instanceProvision() {
        provisions.inc();
    }

    public void instanceDeprovision() {
        deprovisions.inc();
    }

    public void bytesToUdf(long numBytes) {
        bytesToUdf.inc(numBytes);
    }

    public void bytesFromUdf(long numBytes) {
        bytesFromUdf.inc(numBytes);
    }
}
