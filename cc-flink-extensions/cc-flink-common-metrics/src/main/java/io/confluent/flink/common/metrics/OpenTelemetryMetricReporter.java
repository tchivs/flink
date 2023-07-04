/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.LogicalScopeProvider;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.apache.flink.shaded.guava31.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.common.metrics.OpenTelemetryMetricAdapter.CollectionMetadata;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.internal.export.MetricProducer;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A Flink {@link org.apache.flink.metrics.reporter.MetricReporter} which is made to export metrics
 * using Open Telemetry's {@link MetricExporter}.
 */
@Confluent
public class OpenTelemetryMetricReporter implements MetricReporter, MetricProducer, Scheduled {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryMetricReporter.class);

    public static final String ARG_EXPORTER_FACTORY_CLASS = "exporter.factory.class";
    static final String LOGICAL_SCOPE_PREFIX = "flink.";
    static final String OTEL_SERVICE_NAME = "OTEL_SERVICE_NAME";
    static final String OTEL_SERVICE_VERSION = "OTEL_SERVICE_VERSION";

    private final Map<Gauge<?>, MetricMetadata> gauges = new HashMap<>();
    private final Map<Counter, MetricMetadata> counters = new HashMap<>();
    private final Map<Histogram, MetricMetadata> histograms = new HashMap<>();
    private final Map<Meter, MetricMetadata> meters = new HashMap<>();
    private final Resource resource;
    private final Clock clock;
    private MetricExporter exporter;
    // In order to produce deltas, we keep a snapshot of the previous counter collection.
    private Map<Metric, Long> lastValueSnapshots = Collections.emptyMap();
    private long lastCollectTimeNanos = 0;

    public OpenTelemetryMetricReporter() {
        this(Clock.systemUTC());
    }

    @VisibleForTesting
    OpenTelemetryMetricReporter(Clock clock) {
        resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(
                                                ResourceAttributes.SERVICE_NAME,
                                                System.getenv(OTEL_SERVICE_NAME))))
                        .merge(
                                Resource.create(
                                        Attributes.of(
                                                ResourceAttributes.SERVICE_VERSION,
                                                System.getenv(OTEL_SERVICE_VERSION))));
        this.clock = clock;
    }

    @Override
    public void open(MetricConfig metricConfig) {
        LOG.info("Starting OpenTelemetry Metric Reporter");
        exporter =
                createExporter(
                        metricConfig.getString(ARG_EXPORTER_FACTORY_CLASS, null), metricConfig);
    }

    @Override
    public void close() {
        exporter.flush();
        exporter.close();
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String name =
                LOGICAL_SCOPE_PREFIX
                        + LogicalScopeProvider.castFrom(group)
                                .getLogicalScope(CharacterFilter.NO_OP_FILTER)
                        + "."
                        + metricName;

        Map<String, String> variables =
                group.getAllVariables().entrySet().stream()
                        .collect(
                                ImmutableMap.toImmutableMap(
                                        e -> getVariableName(e.getKey()), Entry::getValue));
        Map<String, String> confluentVariables = ConfluentAdapter.adaptVariables(name, variables);
        final String confluentMetricName = ConfluentAdapter.adaptMetricName(name);
        MetricMetadata metricMetadata = new MetricMetadata(confluentMetricName, confluentVariables);

        synchronized (this) {
            switch (metric.getMetricType()) {
                case COUNTER:
                    this.counters.put((Counter) metric, metricMetadata);
                    break;
                case GAUGE:
                    this.gauges.put((Gauge<?>) metric, metricMetadata);
                    break;
                case HISTOGRAM:
                    this.histograms.put((Histogram) metric, metricMetadata);
                    break;
                case METER:
                    this.meters.put((Meter) metric, metricMetadata);
                    break;
                default:
                    LOG.warn(
                            "Cannot add unknown metric type {}. This indicates that the reporter does not "
                                    + "support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
        synchronized (this) {
            // Make sure we're not caching the metric object
            lastValueSnapshots.remove(metric);
            switch (metric.getMetricType()) {
                case COUNTER:
                    this.counters.remove((Counter) metric);
                    break;
                case GAUGE:
                    this.gauges.remove((Gauge<?>) metric);
                    break;
                case HISTOGRAM:
                    this.histograms.remove((Histogram) metric);
                    break;
                case METER:
                    this.meters.remove((Meter) metric);
                    break;
                default:
                    LOG.warn(
                            "Cannot remove unknown metric type {}. This indicates that the reporter does "
                                    + "not support this metric type.",
                            metric.getClass().getName());
            }
        }
    }

    private long getCurrentTimeNanos() {
        Instant now = clock.instant();
        return TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    }

    /**
     * Note that all of the metric data structures in {@link AbstractReporter} are guarded by this,
     * so must make this synchronized.
     *
     * @return The collection of metrics
     */
    @Override
    public synchronized Collection<MetricData> collectAllMetrics() {
        long currentTimeNanos = getCurrentTimeNanos();
        ImmutableList.Builder<MetricData> data = ImmutableList.builder();
        CollectionMetadata collectionMetadata =
                new CollectionMetadata(resource, lastCollectTimeNanos, currentTimeNanos);
        Map<Metric, Long> currentValueSnapshots = takeLastValueSnapshots();
        for (Counter counter : counters.keySet()) {
            Long count = currentValueSnapshots.get(counter);
            Long lastCount = lastValueSnapshots.getOrDefault(counter, 0L);
            MetricMetadata metricMetadata = counters.get(counter);
            Optional<MetricData> metricData =
                    OpenTelemetryMetricAdapter.convertCounter(
                            collectionMetadata, count, lastCount, metricMetadata);
            metricData.ifPresent(data::add);
        }
        for (Gauge<?> gauge : gauges.keySet()) {
            MetricMetadata metricMetadata = gauges.get(gauge);
            Optional<MetricData> metricData =
                    OpenTelemetryMetricAdapter.convertGauge(
                            collectionMetadata, gauge, metricMetadata);
            metricData.ifPresent(data::add);
        }
        for (Meter meter : meters.keySet()) {
            Long count = currentValueSnapshots.get(meter);
            Long lastCount = lastValueSnapshots.getOrDefault(meter, 0L);
            MetricMetadata metricMetadata = meters.get(meter);
            List<MetricData> metricData =
                    OpenTelemetryMetricAdapter.convertMeter(
                            collectionMetadata, meter, count, lastCount, metricMetadata);
            data.addAll(metricData);
        }
        for (Histogram histogram : histograms.keySet()) {
            MetricMetadata metricMetadata = histograms.get(histogram);
            Optional<MetricData> metricData =
                    OpenTelemetryMetricAdapter.convertHistogram(
                            collectionMetadata, histogram, metricMetadata);
            metricData.ifPresent(data::add);
        }
        lastValueSnapshots = currentValueSnapshots;
        lastCollectTimeNanos = currentTimeNanos;
        return data.build();
    }

    private Map<Metric, Long> takeLastValueSnapshots() {
        Map<Metric, Long> map = new HashMap<>();
        for (Counter counter : counters.keySet()) {
            map.put(counter, counter.getCount());
        }
        for (Meter meter : meters.keySet()) {
            map.put(meter, meter.getCount());
        }
        return map;
    }

    private static MetricExporter createExporter(
            String exporterFactoryClassName, MetricConfig metricConfig) {
        if (exporterFactoryClassName == null) {
            throw new IllegalArgumentException("Must set exporter property");
        }
        // Use the same class loader as the one used for this class, assuming they've been
        // loaded together.
        Class<?> clazz;
        try {
            clazz =
                    Class.forName(
                            exporterFactoryClassName,
                            true,
                            OpenTelemetryMetricReporter.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Couldn't find exporter factory: " + exporterFactoryClassName, e);
        }
        if (!MetricExporterFactory.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(
                    "Exporter factory must implement MetricExporterFactory: "
                            + exporterFactoryClassName);
        }
        MetricExporterFactory exporterFactory;
        try {
            exporterFactory = (MetricExporterFactory) clazz.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "Must have no arg constructor: " + exporterFactoryClassName, e);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Error instantiating " + exporterFactoryClassName, e);
        }
        LOG.info("Loaded exporter: " + exporterFactoryClassName);
        return exporterFactory.createMetricExporter(metricConfig);
    }

    @Override
    public void report() {
        Collection<MetricData> metricData = collectAllMetrics();
        try {
            CompletableResultCode result = exporter.export(metricData);
            result.whenComplete(
                    () -> {
                        if (result.isSuccess()) {
                            LOG.debug(
                                    "Exported {} metrics using {}",
                                    metricData.size(),
                                    exporter.getClass().getName());
                        } else {
                            LOG.warn(
                                    "Failed to export {} metrics using {}",
                                    metricData.size(),
                                    exporter.getClass().getName());
                        }
                    });
        } catch (Exception e) {
            LOG.error(
                    "Failed to call export for {} metrics using {}",
                    metricData.size(),
                    exporter.getClass().getName());
        }
    }

    /** Removes leading and trailing angle brackets. */
    private String getVariableName(String str) {
        if (str.startsWith("<") && str.endsWith(">")) {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }
}
