/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.events.Event;
import org.apache.flink.events.reporter.EventReporter;
import org.apache.flink.metrics.MetricConfig;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.logs.LogRecordBuilder;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * A Flink {@link EventReporter} which is made to export log records/events using Open Telemetry's
 * {@link LogRecordExporter}.
 */
@Confluent
public class OpenTelemetryLogRecordReporter extends OpenTelemetryReporterBase
        implements EventReporter {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryLogRecordReporter.class);

    public static final String ARG_SCOPE_VARIABLES_ADDITIONAL = "scope.variables.additional";

    private final Map<String, String> additionalScope = new HashMap<>();
    private LogRecordExporter logRecordExporter;
    private SdkLoggerProvider loggerProvider;
    private BatchLogRecordProcessor logRecordProcessor;

    public OpenTelemetryLogRecordReporter() {
        super();
    }

    @Override
    public void open(MetricConfig metricConfig) {
        open(metricConfig, new OtlpGrpcMetricExporterFactory());
    }

    @VisibleForTesting
    void open(MetricConfig metricConfig, LogRecordExporterFactory logRecordExporterFactory) {
        LOG.info("Starting OpenTelemetry LogRecord Reporter");
        logRecordExporter = logRecordExporterFactory.createLogRecordExporter(metricConfig);
        logRecordProcessor = BatchLogRecordProcessor.builder(logRecordExporter).build();
        loggerProvider =
                SdkLoggerProvider.builder()
                        .addLogRecordProcessor(logRecordProcessor)
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
        logRecordProcessor.forceFlush();
        logRecordProcessor.close();
        logRecordExporter.flush();
        logRecordExporter.close();
    }

    @Override
    public void notifyOfAddedEvent(Event event) {
        io.opentelemetry.api.logs.Logger logger = loggerProvider.get(event.getScope());
        LogRecordBuilder logRecordBuilder = logger.logRecordBuilder();

        event.getAttributes().forEach(setAttribute(logRecordBuilder));
        additionalScope.forEach(setAttribute(logRecordBuilder));

        logRecordBuilder.setObservedTimestamp(event.getObservedTsMillis(), TimeUnit.MILLISECONDS);

        logRecordBuilder.setBody(event.getBody());
        logRecordBuilder.setSeverityText(event.getSeverity());
        try {
            logRecordBuilder.setSeverity(Severity.valueOf(event.getSeverity()));
        } catch (IllegalArgumentException iae) {
            logRecordBuilder.setSeverity(Severity.UNDEFINED_SEVERITY_NUMBER);
        }

        logRecordBuilder.setTimestamp(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        logRecordBuilder.emit();
    }

    private static BiConsumer<String, Object> setAttribute(LogRecordBuilder logRecordBuilder) {
        return (key, value) -> {
            if (value instanceof String) {
                logRecordBuilder.setAttribute(AttributeKey.stringKey(key), (String) value);
            } else if (value instanceof Long) {
                logRecordBuilder.setAttribute(AttributeKey.longKey(key), (Long) value);
            } else if (value instanceof Double) {
                logRecordBuilder.setAttribute(AttributeKey.doubleKey(key), (Double) value);
            } else {
                LOG.warn("Unsupported attribute type [{}={}]", key, value);
            }
        };
    }
}
