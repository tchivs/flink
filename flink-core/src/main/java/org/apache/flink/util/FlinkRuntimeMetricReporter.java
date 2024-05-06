/*
 * Copyright 2024 Confluent Inc.
 */

package org.apache.flink.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerConfluentOptions;
import org.apache.flink.configuration.TaskManagerConfluentOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;

import java.util.Optional;

/** Helper class to simplify FCP Runtime version reporting through a metric group. */
public class FlinkRuntimeMetricReporter {

    public static final String RUNTIME_VERSION_ATTRIBUTE = "runtimeVersion";

    public static void reportTaskManagerRuntimeVersion(
            MetricGroup metricGroup, Configuration configuration) {
        Preconditions.checkNotNull(metricGroup);
        Optional<String> runtimeVersion =
                configuration.getOptional(TaskManagerConfluentOptions.FCP_RUNTIME_VERSION);
        runtimeVersion.ifPresent(s -> addRuntimeMetricSpan(metricGroup, s));
    }

    public static void reportJobManagerRuntimeVersion(
            MetricGroup metricGroup, Configuration configuration) {
        Preconditions.checkNotNull(metricGroup);
        Optional<String> runtimeVersion =
                configuration.getOptional(JobManagerConfluentOptions.FCP_RUNTIME_VERSION);
        runtimeVersion.ifPresent(s -> addRuntimeMetricSpan(metricGroup, s));
    }

    private static void addRuntimeMetricSpan(MetricGroup metricGroup, String runtimeVersion) {
        long now = System.currentTimeMillis();

        // Add runtime version attribute
        SpanBuilder spanBuilder =
                Span.builder(FlinkRuntimeMetricReporter.class, "FlinkRuntime")
                        .setStartTsMillis(now)
                        .setEndTsMillis(now);

        spanBuilder.setAttribute(RUNTIME_VERSION_ATTRIBUTE, runtimeVersion);

        metricGroup.addSpan(spanBuilder);
    }
}
