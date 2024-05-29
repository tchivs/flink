/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.events.Event;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.util.TestLoggerExtension;

import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.flink.common.metrics.OpenTelemetryMetricReporterITCase.ExporterFactory;
import static io.confluent.flink.common.metrics.OpenTelemetryMetricReporterITCase.TestExporter;
import static io.confluent.flink.common.metrics.OpenTelemetryTraceReporter.ARG_EXPORTER_FACTORY_CLASS;
import static io.confluent.flink.common.metrics.OpenTelemetryTraceReporter.ARG_SCOPE_VARIABLES_ADDITIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OpenTelemetryMetricReporter}. */
@ExtendWith({TestLoggerExtension.class})
public class OpenTelemetryLogRecordReporterTest {

    private OpenTelemetryLogRecordReporter reporter;
    private final TestExporter exporter = new TestExporter();

    @BeforeEach
    public void setUp() {
        reporter = new OpenTelemetryLogRecordReporter();

        exporter.reset();
        ExporterFactory.setExporter(exporter);
    }

    @Test
    public void testReportLogRecord() {
        MetricConfig metricConfig = new MetricConfig();
        String scopeKey1 = "scopeKey1";
        String scopeKey2 = "scopeKey2";
        String scopeValue1 = "scopeValue1";
        String scopeValue2 = "scopeValue2";
        String scope = this.getClass().getCanonicalName();
        String attribute1Key = "foo";
        String attribute1Value = "bar";
        String body = "Test!";
        String severity = "INFO";
        long observedTimeMs = 123456L;

        metricConfig.setProperty(
                ARG_SCOPE_VARIABLES_ADDITIONAL,
                String.format("%s: %s , %s : %s", scopeKey1, scopeValue1, scopeKey2, scopeValue2));
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        TestLogRecordExporterFactory logRecordExporterFactory = new TestLogRecordExporterFactory();
        reporter.open(metricConfig, logRecordExporterFactory);
        try {
            reporter.notifyOfAddedEvent(
                    Event.builder(this.getClass())
                            .setAttribute(attribute1Key, attribute1Value)
                            .setBody(body)
                            .setObservedTsMillis(observedTimeMs)
                            .setSeverity(severity)
                            .build());
        } finally {
            reporter.close();
        }

        assertThat(logRecordExporterFactory.getLogRecords()).hasSize(1);
        LogRecordData actualSpanData = logRecordExporterFactory.getLogRecords().get(0);
        assertThat(actualSpanData.getInstrumentationScopeInfo().getName()).isEqualTo(scope);
        assertThat(actualSpanData.getSeverityText()).isEqualTo(severity);
        assertThat(actualSpanData.getSeverity()).isEqualTo(Severity.INFO);
        assertThat(actualSpanData.getBody().asString()).isEqualTo(body);
        assertThat(actualSpanData.getObservedTimestampEpochNanos())
                .isEqualTo(1_000_000L * observedTimeMs);

        Map<String, String> attributes = new HashMap<>();
        actualSpanData
                .getAttributes()
                .asMap()
                .forEach((key, value) -> attributes.put(key.getKey(), value.toString()));

        Map<String, String> expectedAttributes = new HashMap<>();
        expectedAttributes.put(scopeKey1, scopeValue1);
        expectedAttributes.put(scopeKey2, scopeValue2);
        expectedAttributes.put(attribute1Key, attribute1Value);
        assertThat(attributes).containsExactlyInAnyOrderEntriesOf(expectedAttributes);
    }

    static class TestLogRecordExporterFactory implements LogRecordExporterFactory {
        private final List<LogRecordData> logRecords = new ArrayList<>();

        public List<LogRecordData> getLogRecords() {
            return logRecords;
        }

        @Override
        public LogRecordExporter createLogRecordExporter(MetricConfig metricConfig) {
            return new TestLogRecordExporter(logRecords);
        }
    }

    static class TestLogRecordExporter implements LogRecordExporter {

        private final Collection<LogRecordData> logRecords;

        public TestLogRecordExporter(Collection<LogRecordData> logRecords) {
            this.logRecords = logRecords;
        }

        @Override
        public CompletableResultCode export(Collection<LogRecordData> logRecords) {
            this.logRecords.addAll(logRecords);
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }
}
