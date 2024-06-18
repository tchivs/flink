/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.TestLoggerExtension;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.flink.common.metrics.OpenTelemetryMetricReporterITCase.ExporterFactory;
import static io.confluent.flink.common.metrics.OpenTelemetryMetricReporterITCase.TestExporter;
import static io.confluent.flink.common.metrics.OpenTelemetryTraceReporter.ARG_EXPORTER_FACTORY_CLASS;
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
    public void testReportNestedSpan() {
        MetricConfig metricConfig = new MetricConfig();

        String scope = this.getClass().getCanonicalName();

        String attribute1KeyRoot = "foo";
        String attribute1ValueRoot = "bar";
        String attribute2KeyRoot = "<variable>";
        String attribute2ValueRoot = "value";

        String attribute1KeyL1N1 = "foo_1_1";
        String attribute1ValueL1N1 = "bar_1_1";

        String attribute1KeyL1N2 = "foo_1_2";
        String attribute1ValueL1N2 = "bar_1_2";

        String attribute1KeyL2N1 = "foo_2_1";
        String attribute1ValueL2N1 = "bar_2_1";

        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        TestSpanExporterFactory spanExporterFactory = new TestSpanExporterFactory();
        reporter.open(metricConfig, spanExporterFactory);
        try {
            SpanBuilder childLeveL2N1 =
                    Span.builder(this.getClass(), "2_1")
                            .setAttribute(attribute1KeyL2N1, attribute1ValueL2N1)
                            .setStartTsMillis(44)
                            .setEndTsMillis(46);

            SpanBuilder childL1N1 =
                    Span.builder(this.getClass(), "1_1")
                            .setAttribute(attribute1KeyL1N1, attribute1ValueL1N1)
                            .setStartTsMillis(43)
                            .setEndTsMillis(48)
                            .addChild(childLeveL2N1);

            SpanBuilder childL1N2 =
                    Span.builder(this.getClass(), "1_2")
                            .setAttribute(attribute1KeyL1N2, attribute1ValueL1N2)
                            .setStartTsMillis(44)
                            .setEndTsMillis(46);

            SpanBuilder rootSpan =
                    Span.builder(this.getClass(), "root")
                            .setAttribute(attribute1KeyRoot, attribute1ValueRoot)
                            .setAttribute(attribute2KeyRoot, attribute2ValueRoot)
                            .setStartTsMillis(42)
                            .setEndTsMillis(64)
                            .addChildren(Arrays.asList(childL1N1, childL1N2));

            reporter.notifyOfAddedSpan(rootSpan.build());
        } finally {
            reporter.close();
        }

        assertThat(spanExporterFactory.getSpans()).hasSize(4);

        SpanData spanDataRoot = spanExporterFactory.getSpans().get(3);
        assertThat(spanDataRoot.getName()).isEqualTo("root");
        assertThat(spanDataRoot.getInstrumentationScopeInfo().getName()).isEqualTo(scope);
        Map<String, String> attributes = new HashMap<>();
        spanDataRoot
                .getAttributes()
                .asMap()
                .forEach((key, value) -> attributes.put(key.getKey(), value.toString()));

        Map<String, String> expected = CollectionUtil.newHashMapWithExpectedSize(2);
        expected.put(attribute1KeyRoot, attribute1ValueRoot);
        expected.put(
                attribute2KeyRoot.substring(1, attribute2KeyRoot.length() - 1),
                attribute2ValueRoot);
        assertThat(attributes).containsExactlyInAnyOrderEntriesOf(expected);
        assertThat(spanDataRoot.getParentSpanId()).isEqualTo("0000000000000000");

        SpanData spanDataL1N2 = spanExporterFactory.getSpans().get(2);
        assertThat(spanDataL1N2.getName()).isEqualTo("1_2");
        assertThat(spanDataL1N2.getInstrumentationScopeInfo().getName()).isEqualTo(scope);
        attributes.clear();
        spanDataL1N2
                .getAttributes()
                .asMap()
                .forEach((key, value) -> attributes.put(key.getKey(), value.toString()));

        assertThat(attributes)
                .containsExactlyInAnyOrderEntriesOf(
                        Collections.singletonMap(attribute1KeyL1N2, attribute1ValueL1N2));
        assertThat(spanDataL1N2.getParentSpanId()).isEqualTo(spanDataRoot.getSpanId());

        SpanData spanDataL1N1 = spanExporterFactory.getSpans().get(1);
        assertThat(spanDataL1N1.getName()).isEqualTo("1_1");
        assertThat(spanDataL1N1.getInstrumentationScopeInfo().getName()).isEqualTo(scope);
        attributes.clear();
        spanDataL1N1
                .getAttributes()
                .asMap()
                .forEach((key, value) -> attributes.put(key.getKey(), value.toString()));

        assertThat(attributes)
                .containsExactlyInAnyOrderEntriesOf(
                        Collections.singletonMap(attribute1KeyL1N1, attribute1ValueL1N1));
        assertThat(spanDataL1N1.getParentSpanId()).isEqualTo(spanDataRoot.getSpanId());

        SpanData spanDataL2N1 = spanExporterFactory.getSpans().get(0);
        assertThat(spanDataL2N1.getName()).isEqualTo("2_1");
        assertThat(spanDataL2N1.getInstrumentationScopeInfo().getName()).isEqualTo(scope);
        attributes.clear();
        spanDataL2N1
                .getAttributes()
                .asMap()
                .forEach((key, value) -> attributes.put(key.getKey(), value.toString()));

        assertThat(attributes)
                .containsExactlyInAnyOrderEntriesOf(
                        Collections.singletonMap(attribute1KeyL2N1, attribute1ValueL2N1));
        assertThat(spanDataL2N1.getParentSpanId()).isEqualTo(spanDataL1N1.getSpanId());
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
