/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.reporter.TraceReporter;

import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * A Flink {@link org.apache.flink.metrics.reporter.MetricReporter} which is made to export metrics
 * using Open Telemetry's {@link MetricExporter}.
 */
@Confluent
public class OpenTelemetryTraceReporter extends OpenTelemetryReporterBase implements TraceReporter {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryTraceReporter.class);

    public static final String ARG_SCOPE_VARIABLES_ADDITIONAL = "scope.variables.additional";

    private final Map<String, String> additionalScope = new HashMap<>();
    private SpanExporter spanExporter;
    private TracerProvider tracerProvider;
    private BatchSpanProcessor spanProcessor;

    public OpenTelemetryTraceReporter() {
        super();
    }

    @Override
    public void open(MetricConfig metricConfig) {
        open(metricConfig, new OtlpGrpcMetricExporterFactory());
    }

    @VisibleForTesting
    void open(MetricConfig metricConfig, SpanExporterFactory spanExporterFactory) {
        LOG.info("Starting OpenTelemetry Metric Reporter");
        spanExporter = spanExporterFactory.createSpanExporter(metricConfig);
        spanProcessor = BatchSpanProcessor.builder(spanExporter).build();
        tracerProvider =
                SdkTracerProvider.builder()
                        .addSpanProcessor(spanProcessor)
                        .setResource(resource)
                        .build();

        String additionalScopeVariables =
                metricConfig.getString(ARG_SCOPE_VARIABLES_ADDITIONAL, "");
        for (String scope : additionalScopeVariables.split(",")) {
            if (scope.length() == 0) {
                continue;
            }
            String[] keyValue = scope.split(":");
            if (keyValue.length != 2) {
                LOG.warn(
                        "unable to parse [{}] from config [{} = {}]",
                        scope,
                        ARG_SCOPE_VARIABLES_ADDITIONAL,
                        additionalScopeVariables);
                continue;
            }
            additionalScope.put(keyValue[0].trim(), keyValue[1].trim());
        }
    }

    @Override
    public void close() {
        spanProcessor.forceFlush();
        spanProcessor.close();
        spanExporter.flush();
        spanExporter.close();
    }

    @Override
    public void notifyOfAddedSpan(Span span) {
        Tracer tracer = tracerProvider.get(span.getScope());
        SpanBuilder spanBuilder = tracer.spanBuilder(span.getName());

        span.getAttributes().forEach(setAttribute(spanBuilder));
        additionalScope.forEach(setAttribute(spanBuilder));

        spanBuilder
                .setStartTimestamp(span.getStartTsMillis(), TimeUnit.MILLISECONDS)
                .setNoParent()
                .startSpan()
                .end(span.getEndTsMillis(), TimeUnit.MILLISECONDS);
    }

    private static BiConsumer<String, Object> setAttribute(SpanBuilder spanBuilder) {
        return (key, value) -> {
            if (value instanceof String) {
                spanBuilder.setAttribute(key, (String) value);
            } else if (value instanceof Long) {
                spanBuilder.setAttribute(key, (Long) value);
            } else if (value instanceof Double) {
                spanBuilder.setAttribute(key, (Double) value);
            } else {
                LOG.warn("Unsupported attribute type [{}={}]", key, value);
            }
        };
    }
}
