/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Properties;

/** A factory for creating a {@link OpenTelemetryMetricReporter}. */
@Confluent
public class OpenTelemetryMetricReporterFactory implements MetricReporterFactory {

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new OpenTelemetryMetricReporter();
    }
}
