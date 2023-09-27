/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.traces.Span;
import org.apache.flink.util.TestLoggerExtension;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
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
public class OpenTelemetryTraceReporterTest {

    private OpenTelemetryTraceReporter reporter;
    private final TestExporter exporter = new TestExporter();

    @BeforeEach
    public void setUp() {
        reporter = new OpenTelemetryTraceReporter();

        exporter.reset();
        ExporterFactory.setExporter(exporter);
    }

    @Test
    public void testReportSpan() {
        MetricConfig metricConfig = new MetricConfig();
        String scopeKey1 = "scopeKey1";
        String scopeKey2 = "scopeKey2";
        String scopeValue1 = "scopeValue1";
        String scopeValue2 = "scopeValue2";
        String scope = this.getClass().getCanonicalName();
        String name = "name";
        String attribute1Key = "foo";
        String attribute1Value = "bar";

        metricConfig.setProperty(
                ARG_SCOPE_VARIABLES_ADDITIONAL,
                String.format("%s: %s , %s : %s", scopeKey1, scopeValue1, scopeKey2, scopeValue2));
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        TestSpanExporterFactory spanExporterFactory = new TestSpanExporterFactory();
        reporter.open(metricConfig, spanExporterFactory);
        try {
            reporter.notifyOfAddedSpan(
                    Span.builder(this.getClass(), name)
                            .setAttribute(attribute1Key, attribute1Value)
                            .setStartTsMillis(42)
                            .setEndTsMillis(44)
                            .build());
        } finally {
            reporter.close();
        }

        assertThat(spanExporterFactory.getSpans()).hasSize(1);
        SpanData actualSpanData = spanExporterFactory.getSpans().get(0);
        assertThat(actualSpanData.getName()).isEqualTo(name);
        assertThat(actualSpanData.getInstrumentationScopeInfo().getName()).isEqualTo(scope);
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

    static class TestSpanExporterFactory implements SpanExporterFactory {
        private final List<SpanData> spans = new ArrayList<>();

        public List<SpanData> getSpans() {
            return spans;
        }

        @Override
        public SpanExporter createSpanExporter(MetricConfig metricConfig) {
            return new TestSpanExporter(spans);
        }
    }

    static class TestSpanExporter implements SpanExporter {

        private final Collection<SpanData> spans;

        public TestSpanExporter(Collection<SpanData> spans) {
            this.spans = spans;
        }

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            this.spans.addAll(spans);
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
