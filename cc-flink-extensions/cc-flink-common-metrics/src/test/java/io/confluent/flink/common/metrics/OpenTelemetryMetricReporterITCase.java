/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.util.TestHistogram;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryData;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static io.confluent.flink.common.metrics.OpenTelemetryMetricReporter.ARG_EXPORTER_FACTORY_CLASS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** Tests for {@link OpenTelemetryMetricReporter}. */
public class OpenTelemetryMetricReporterITCase {

    private static final long TIME_MS = 1234;

    private OpenTelemetryMetricReporter reporter;
    private final TestExporter exporter = new TestExporter();
    private final Histogram histogram = new TestHistogram();

    @BeforeEach
    public void setUp() {
        reporter =
                new OpenTelemetryMetricReporter(
                        Clock.fixed(Instant.ofEpochMilli(TIME_MS), Clock.systemUTC().getZone()));

        exporter.reset();
        ExporterFactory.setExporter(exporter);
    }

    @Test
    public void testReport() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        Gauge<Double> gauge = () -> 123.456d;
        reporter.notifyOfAddedMetric(gauge, "foo.gauge", group);

        MeterView meter = new MeterView(counter);
        reporter.notifyOfAddedMetric(meter, "foo.meter", group);

        reporter.notifyOfAddedMetric(histogram, "foo.histogram", group);

        reporter.report();
        reporter.close();

        List<MetricData> metricData = new ArrayList<>(exporter.getLastCollection());
        assertThat(metricData.size()).isEqualTo(5);
        assertThat(metricData.get(0).getLongSumData()).isNotEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get(1).getDoubleGaugeData()).isNotEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get(2).getLongSumData()).isNotEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get(3).getDoubleGaugeData()).isNotEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get(4).getSummaryData()).isNotEqualTo(ImmutableSummaryData.empty());
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    @Test
    public void testNaming() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        Gauge<Double> gauge = () -> 123.456d;
        reporter.notifyOfAddedMetric(gauge, "foo.gauge", group);

        MeterView meter = new MeterView(counter);
        reporter.notifyOfAddedMetric(meter, "foo.meter", group);

        reporter.notifyOfAddedMetric(histogram, "foo.histogram", group);

        reporter.report();
        reporter.close();

        List<MetricData> metricData = new ArrayList<>(exporter.getLastCollection());
        assertThat(metricData.size()).isEqualTo(5);
        assertThat(metricData.get(0).getName()).isEqualTo("flink.logical.scope.foo.counter");
        assertThat(metricData.get(1).getName()).isEqualTo("flink.logical.scope.foo.gauge");
        assertThat(metricData.get(2).getName()).isEqualTo("flink.logical.scope.foo.meter.count");
        assertThat(metricData.get(3).getName()).isEqualTo("flink.logical.scope.foo.meter.rate");
        assertThat(metricData.get(4).getName()).isEqualTo("flink.logical.scope.foo.histogram");
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    @Test
    public void testVariablesAdaptedByConfluentAdapter() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "currentInput12Watermark.counter", group);

        reporter.report();
        reporter.close();

        List<MetricData> metricData = new ArrayList<>(exporter.getLastCollection());
        assertThat(metricData.get(0).getName())
                .isEqualTo("flink.logical.scope.currentInputNWatermark.counter");
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    @Test
    public void testReportAfterUnregister() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter1 = new SimpleCounter();
        SimpleCounter counter2 = new SimpleCounter();
        SimpleCounter counter3 = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter1, "foo.counter1", group);
        reporter.notifyOfAddedMetric(counter2, "foo.counter2", group);
        reporter.notifyOfAddedMetric(counter3, "foo.counter3", group);

        reporter.notifyOfRemovedMetric(counter2, "foo.counter2", group);

        reporter.report();
        reporter.close();

        List<MetricData> metricData = new ArrayList<>(exporter.getLastCollection());
        assertThat(metricData.size()).isEqualTo(2);
        assertThat(metricData.get(0).getLongSumData()).isNotEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get(1).getLongSumData()).isNotEqualTo(ImmutableSumData.empty());
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    @Test
    public void testBadExporterClass_name() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, "bad.class.name");

        IllegalArgumentException exception =
                catchThrowableOfType(
                        () -> reporter.open(metricConfig), IllegalArgumentException.class);
        assertThat(exception).hasMessageContaining("Couldn't find exporter factory");
    }

    @Test
    public void testBadExporterClass_noConstructor() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(
                ARG_EXPORTER_FACTORY_CLASS, NoZeroConstructorExporterFactory.class.getName());

        IllegalArgumentException exception =
                catchThrowableOfType(
                        () -> reporter.open(metricConfig), IllegalArgumentException.class);
        assertThat(exception).hasMessageContaining("Must have no arg constructo");
    }

    @Test
    public void testBadExporterClass_notSet() {
        MetricConfig metricConfig = new MetricConfig();

        IllegalArgumentException exception =
                catchThrowableOfType(
                        () -> reporter.open(metricConfig), IllegalArgumentException.class);
        assertThat(exception).hasMessageContaining("Must set exporter property");
    }

    @Test
    public void testBadExporterClass_notExporterFactory() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(
                ARG_EXPORTER_FACTORY_CLASS, NotReallyAnExporterFactory.class.getName());

        IllegalArgumentException exception =
                catchThrowableOfType(
                        () -> reporter.open(metricConfig), IllegalArgumentException.class);
        assertThat(exception)
                .hasMessageContaining("Exporter factory must implement MetricExporterFactory");
    }

    @Test
    public void testFailedExport_resultFailure() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        exporter.setExportResultCodeSupplier(CompletableResultCode::ofFailure);

        reporter.open(metricConfig);

        reporter.report();
        reporter.close();

        assertThat(exporter.getLastCollection()).isNotNull();
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    @Test
    public void testFailedExport_exception() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        exporter.setExportResultCodeSupplier(
                () -> {
                    throw new RuntimeException("Error!");
                });

        reporter.open(metricConfig);

        reporter.report();
        reporter.close();

        assertThat(exporter.getLastCollection()).isNotNull();
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    @Test
    public void testCounterDelta() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        counter.inc(1234);
        assertThat(counter.getCount()).isEqualTo(1234L);
        reporter.report();
        counter.inc(25);
        assertThat(counter.getCount()).isEqualTo(1259L);
        reporter.report();

        reporter.close();

        List<Collection<MetricData>> allData = exporter.getAllCollection();
        List<MetricData> metricData1 = new ArrayList<>(allData.get(0));
        List<MetricData> metricData2 = new ArrayList<>(allData.get(1));

        assertThat(metricData1.size()).isEqualTo(1);
        assertThat(metricData1.get(0).getLongSumData()).isNotEqualTo(ImmutableSumData.empty());
        assertThat(metricData1.get(0).getLongSumData().getAggregationTemporality())
                .isEqualTo(AggregationTemporality.DELTA);
        assertThat(metricData1.get(0).getLongSumData().getPoints().iterator().next().getValue())
                .isEqualTo(1234L);

        assertThat(metricData2.size()).isEqualTo(1);
        assertThat(metricData2.get(0).getLongSumData()).isNotEqualTo(ImmutableSumData.empty());
        assertThat(metricData2.get(0).getLongSumData().getAggregationTemporality())
                .isEqualTo(AggregationTemporality.DELTA);
        assertThat(metricData2.get(0).getLongSumData().getPoints().iterator().next().getValue())
                .isEqualTo(25L);
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    @Test
    public void testMeterDelta() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        MeterView meter = new MeterView(counter);
        reporter.notifyOfAddedMetric(meter, "foo.meter", group);

        counter.inc(1234);
        assertThat(counter.getCount()).isEqualTo(1234L);
        reporter.report();
        counter.inc(25);
        assertThat(counter.getCount()).isEqualTo(1259L);
        reporter.report();

        reporter.close();

        List<Collection<MetricData>> allData = exporter.getAllCollection();
        List<MetricData> metricData1 = new ArrayList<>(allData.get(0));
        List<MetricData> metricData2 = new ArrayList<>(allData.get(1));

        assertThat(metricData1.size()).isEqualTo(2);
        assertThat(metricData1.get(0).getLongSumData()).isNotEqualTo(ImmutableSumData.empty());
        assertThat(metricData1.get(0).getLongSumData().getAggregationTemporality())
                .isEqualTo(AggregationTemporality.DELTA);
        assertThat(metricData1.get(0).getLongSumData().getPoints().iterator().next().getValue())
                .isEqualTo(1234L);

        assertThat(metricData2.size()).isEqualTo(2);
        assertThat(metricData2.get(0).getLongSumData()).isNotEqualTo(ImmutableSumData.empty());
        assertThat(metricData2.get(0).getLongSumData().getAggregationTemporality())
                .isEqualTo(AggregationTemporality.DELTA);
        assertThat(metricData2.get(0).getLongSumData().getPoints().iterator().next().getValue())
                .isEqualTo(25L);
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    static class ExporterFactory implements MetricExporterFactory {

        private static MetricExporter exporter;

        public ExporterFactory() {}

        public static void setExporter(MetricExporter exporter) {
            ExporterFactory.exporter = exporter;
        }

        @Override
        public MetricExporter createMetricExporter(MetricConfig metricConfig) {
            return exporter;
        }
    }

    private static class NoZeroConstructorExporterFactory implements MetricExporterFactory {

        public NoZeroConstructorExporterFactory(String arg) {}

        @Override
        public MetricExporter createMetricExporter(MetricConfig metricConfig) {
            return null;
        }
    }

    private static class NotReallyAnExporterFactory {

        public NotReallyAnExporterFactory() {}
    }

    private static class TestMetricGroup extends UnregisteredMetricsGroup
            implements LogicalScopeProvider {

        @Override
        public String getLogicalScope(CharacterFilter characterFilter) {
            return "logical.scope";
        }

        @Override
        public String getLogicalScope(CharacterFilter characterFilter, char c) {
            return "logical.scope";
        }

        @Override
        public MetricGroup getWrappedMetricGroup() {
            return this;
        }
    }

    @Test
    public void testOtelAttributes() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_FACTORY_CLASS, ExporterFactory.class.getName());
        MetricGroup group = new TestMetricGroup();

        reporter.open(metricConfig);

        SimpleCounter counter = new SimpleCounter();
        reporter.notifyOfAddedMetric(counter, "foo.counter", group);

        reporter.report();
        reporter.close();

        List<MetricData> metricData = new ArrayList<>(exporter.getLastCollection());
        assertThat(metricData.get(0).getResource().getAttribute(ResourceAttributes.SERVICE_NAME))
                .isEqualTo("flink");
        assertThat(metricData.get(0).getResource().getAttribute(ResourceAttributes.SERVICE_VERSION))
                .isEqualTo("some-test-version");
        assertThat(exporter.isFlushCalled()).isTrue();
        assertThat(exporter.isShutdownCalled()).isTrue();
    }

    private static class TestExporter implements MetricExporter {

        private static final Supplier<CompletableResultCode> DEFAULT_EXPORT_RESULT_SUPPLIER =
                CompletableResultCode::ofSuccess;

        private Supplier<CompletableResultCode> exportResultCodeSupplier =
                DEFAULT_EXPORT_RESULT_SUPPLIER;
        private List<Collection<MetricData>> collections = new ArrayList<>();

        private boolean flushCalled = false;
        private boolean shutdownCalled = false;

        public void setExportResultCodeSupplier(
                Supplier<CompletableResultCode> exportResultCodeSupplier) {
            this.exportResultCodeSupplier = exportResultCodeSupplier;
        }

        public Collection<MetricData> getLastCollection() {
            return collections.get(collections.size() - 1);
        }

        public List<Collection<MetricData>> getAllCollection() {
            return collections;
        }

        public boolean isFlushCalled() {
            return flushCalled;
        }

        public boolean isShutdownCalled() {
            return shutdownCalled;
        }

        public void reset() {
            exportResultCodeSupplier = DEFAULT_EXPORT_RESULT_SUPPLIER;
            collections = new ArrayList<>();
            flushCalled = false;
            shutdownCalled = false;
        }

        @Override
        public CompletableResultCode export(Collection<MetricData> collection) {
            collections.add(collection);
            return exportResultCodeSupplier.get();
        }

        @Override
        public CompletableResultCode flush() {
            flushCalled = true;
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            shutdownCalled = true;
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
            return AggregationTemporality.DELTA;
        }
    }
}
