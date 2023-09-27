/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.util.TimeUtils;

import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporterBuilder;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporterBuilder;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A factory for creating a {@link OtlpGrpcMetricExporter}. */
@Confluent
public class OtlpGrpcMetricExporterFactory implements MetricExporterFactory, SpanExporterFactory {
    private static final Logger LOG = LoggerFactory.getLogger(OtlpGrpcMetricExporterFactory.class);

    public static final String ARG_EXPORTER_ENDPOINT = "exporter.endpoint";
    public static final String OTEL_EXPORTER_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT";
    public static final String ARG_EXPORTER_TIMEOUT = "exporter.timeout";

    public OtlpGrpcMetricExporterFactory() {}

    @Override
    public MetricExporter createMetricExporter(MetricConfig metricConfig) {
        OtlpGrpcMetricExporterBuilder builder =
                OtlpGrpcMetricExporter.builder().setEndpoint(tryGetEndpoint(metricConfig));
        if (metricConfig.containsKey(ARG_EXPORTER_TIMEOUT)) {
            builder.setTimeout(
                    TimeUtils.parseDuration(metricConfig.getProperty(ARG_EXPORTER_TIMEOUT)));
        }
        return builder.build();
    }

    @Override
    public SpanExporter createSpanExporter(MetricConfig metricConfig) {
        OtlpGrpcSpanExporterBuilder builder =
                OtlpGrpcSpanExporter.builder().setEndpoint(tryGetEndpoint(metricConfig));
        if (metricConfig.containsKey(ARG_EXPORTER_TIMEOUT)) {
            builder.setTimeout(
                    TimeUtils.parseDuration(metricConfig.getProperty(ARG_EXPORTER_TIMEOUT)));
        }
        return builder.build();
    }

    private String tryGetEndpoint(MetricConfig metricConfig) {
        if (metricConfig.containsKey(ARG_EXPORTER_ENDPOINT)) {
            return metricConfig.getProperty(ARG_EXPORTER_ENDPOINT);
        }
        if (System.getenv(OTEL_EXPORTER_ENDPOINT_ENV) != null) {
            LOG.info(
                    "exporter.endpoint was not set, defaulting to env OTEL_EXPORTER_OTLP_ENDPOINT");
            return System.getenv(OTEL_EXPORTER_ENDPOINT_ENV);
        }
        throw new IllegalArgumentException(
                "Must set exporter.endpoint or"
                        + " OTEL_EXPORTER_OTLP_ENDPOINT env for OtlpGrpcMetricExporter");
    }
}
