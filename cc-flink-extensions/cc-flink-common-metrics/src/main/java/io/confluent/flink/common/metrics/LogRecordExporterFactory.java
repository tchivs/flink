/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.metrics.MetricConfig;

import io.opentelemetry.sdk.logs.export.LogRecordExporter;

/**
 * An interface for creating {@link LogRecordExporter}s. There must exist such a factory to be used
 * with this reporter.
 */
@Confluent
public interface LogRecordExporterFactory {

    /**
     * Creates a {@link LogRecordExporter}.
     *
     * @param metricConfig The {@link MetricConfig} for the subset of Flink properties passed to the
     *     {@link OpenTelemetryMetricReporter}. For example,
     *     "metrics.reporter.otel_reporter.exporter.a" and
     *     "metrics.reporter.otel_reporter.exporter.b" are passed as "exporter.a" and "exporter.b".
     * @return The created exporter
     */
    LogRecordExporter createLogRecordExporter(MetricConfig metricConfig);
}
