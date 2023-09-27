/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.traces.reporter.TraceReporter;
import org.apache.flink.traces.reporter.TraceReporterFactory;

import java.util.Properties;

/** A factory for creating a {@link OpenTelemetryMetricReporter}. */
@Confluent
public class OpenTelemetryTraceReporterFactory implements TraceReporterFactory {
    @Override
    public TraceReporter createTraceReporter(Properties properties) {
        return new OpenTelemetryTraceReporter();
    }
}
