/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.metrics.MetricConfig;

import io.opentelemetry.sdk.trace.export.SpanExporter;

/**
 * An interface for creating {@link SpanExporter}s. There must exist such a factory to be used with
 * this reporter.
 */
@Confluent
public interface SpanExporterFactory {

    /**
     * Creates a {@link SpanExporter}.
     *
     * @param metricConfig The {@link MetricConfig} for the subset of Flink properties passed to the
     *     {@link OpenTelemetryMetricReporter}. For example,
     *     "metrics.reporter.otel_reporter.exporter.a" and
     *     "metrics.reporter.otel_reporter.exporter.b" are passed as "exporter.a" and "exporter.b".
     * @return The created exporter
     */
    SpanExporter createSpanExporter(MetricConfig metricConfig);
}
