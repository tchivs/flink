/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.Streams;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.ValueAtQuantile;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableValueAtQuantile;
import io.opentelemetry.sdk.resources.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An adapter class which translates from Flink metrics to Otel metrics which can exported with the
 * standard Otel {@link io.opentelemetry.sdk.metrics.export.MetricExporter}s.
 */
@Confluent
class OpenTelemetryMetricAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryMetricAdapter.class);
    static final double[] HISTOGRAM_QUANTILES = {0.5, 0.75, 0.95, 0.99};

    static final InstrumentationScopeInfo INSTRUMENTATION_SCOPE_INFO =
            InstrumentationScopeInfo.create("io.confluent.flink.common.metrics");

    public static Optional<MetricData> convertCounter(
            CollectionMetadata collectionMetadata,
            Long count,
            Long previousCount,
            MetricMetadata metricMetadata) {
        long delta = count - previousCount;
        return Optional.of(
                ImmutableMetricData.createLongSum(
                        collectionMetadata.getOtelResource(),
                        INSTRUMENTATION_SCOPE_INFO,
                        metricMetadata.getName(),
                        "",
                        "",
                        ImmutableSumData.create(
                                false,
                                AggregationTemporality.DELTA,
                                ImmutableList.of(
                                        ImmutableLongPointData.create(
                                                collectionMetadata.getStartEpochNanos(),
                                                collectionMetadata.getEpochNanos(),
                                                convertVariables(metricMetadata.getVariables()),
                                                delta)))));
    }

    /**
     * Converts a Flink Gauge to a {@link MetricData}.
     *
     * @param collectionMetadata The common collection metadata
     * @param gauge The Flink Gauge to convert
     * @param metricMetadata The metric metadata
     * @return A {@link MetricData} if it's able to convert successfully
     */
    public static Optional<MetricData> convertGauge(
            CollectionMetadata collectionMetadata, Gauge<?> gauge, MetricMetadata metricMetadata) {
        if (!(gauge.getValue() instanceof Number)) {
            LOG.debug(
                    "Couldn't adapt gauge {} with value {} and type {}",
                    metricMetadata.getName(),
                    gauge.getValue(),
                    gauge.getValue().getClass().getName());
            return Optional.empty();
        }
        Number number = (Number) gauge.getValue();
        if (number instanceof Long || number instanceof Integer) {
            return Optional.of(
                    ImmutableMetricData.createLongGauge(
                            collectionMetadata.getOtelResource(),
                            INSTRUMENTATION_SCOPE_INFO,
                            metricMetadata.getName(),
                            "",
                            "",
                            ImmutableGaugeData.create(
                                    ImmutableList.of(
                                            ImmutableLongPointData.create(
                                                    collectionMetadata.getStartEpochNanos(),
                                                    collectionMetadata.getEpochNanos(),
                                                    convertVariables(metricMetadata.getVariables()),
                                                    number.longValue())))));
        } else {
            return Optional.of(
                    ImmutableMetricData.createDoubleGauge(
                            collectionMetadata.getOtelResource(),
                            INSTRUMENTATION_SCOPE_INFO,
                            metricMetadata.getName(),
                            "",
                            "",
                            ImmutableGaugeData.create(
                                    ImmutableList.of(
                                            ImmutableDoublePointData.create(
                                                    collectionMetadata.getStartEpochNanos(),
                                                    collectionMetadata.getEpochNanos(),
                                                    convertVariables(metricMetadata.getVariables()),
                                                    number.doubleValue())))));
        }
    }

    /**
     * Converts a Flink Meter to a {@link MetricData}.
     *
     * @param collectionMetadata The common collection metadata
     * @param meter The Flink Meter to convert
     * @param metricMetadata The metric metadata
     * @return A {@link MetricData} if it's able to convert successfully
     */
    public static List<MetricData> convertMeter(
            CollectionMetadata collectionMetadata,
            Meter meter,
            Long count,
            Long previousCount,
            MetricMetadata metricMetadata) {
        List<MetricData> metricData = new ArrayList<>();
        convertCounter(collectionMetadata, count, previousCount, metricMetadata.subMetric("count"))
                .ifPresent(metricData::add);
        convertGauge(collectionMetadata, meter::getRate, metricMetadata.subMetric("rate"))
                .ifPresent(metricData::add);
        return metricData;
    }

    /**
     * Converts a Flink Histogram to a list of {@link MetricData}s.
     *
     * @param collectionMetadata The common collection metadata
     * @param histogram The Flink Histogram to convert
     * @param metricMetadata The metric metadata
     * @return A list of {@link MetricData}s if it's able to convert successfully, or empty if not
     */
    public static Optional<MetricData> convertHistogram(
            CollectionMetadata collectionMetadata,
            Histogram histogram,
            MetricMetadata metricMetadata) {
        List<ValueAtQuantile> quantileList =
                Streams.concat(
                                ImmutableList.of(
                                        ImmutableValueAtQuantile.create(
                                                0, histogram.getStatistics().getMin()))
                                        .stream(),
                                Arrays.stream(HISTOGRAM_QUANTILES)
                                        .mapToObj(
                                                q ->
                                                        ImmutableValueAtQuantile.create(
                                                                q,
                                                                histogram
                                                                        .getStatistics()
                                                                        .getQuantile(q))),
                                ImmutableList.of(
                                        ImmutableValueAtQuantile.create(
                                                1, histogram.getStatistics().getMax()))
                                        .stream())
                        .collect(ImmutableList.toImmutableList());
        return Optional.of(
                ImmutableMetricData.createDoubleSummary(
                        collectionMetadata.getOtelResource(),
                        INSTRUMENTATION_SCOPE_INFO,
                        metricMetadata.getName(),
                        "",
                        "",
                        ImmutableSummaryData.create(
                                ImmutableList.of(
                                        ImmutableSummaryPointData.create(
                                                collectionMetadata.getStartEpochNanos(),
                                                collectionMetadata.getEpochNanos(),
                                                convertVariables(metricMetadata.getVariables()),
                                                histogram.getCount(),
                                                histogram.getStatistics().getMean()
                                                        * histogram.getCount(),
                                                quantileList)))));
    }

    private static Attributes convertVariables(Map<String, String> variables) {
        AttributesBuilder builder = Attributes.builder();
        variables.forEach(builder::put);
        return builder.build();
    }

    /** The common metadata associated with a collection of the metrics. */
    public static class CollectionMetadata {

        private final Resource otelResource;
        private final long startEpochNanos;
        private final long epochNanos;

        public CollectionMetadata(Resource otelResource, long startEpochNanos, long epochNanos) {
            this.otelResource = otelResource;
            this.startEpochNanos = startEpochNanos;
            this.epochNanos = epochNanos;
        }

        public Resource getOtelResource() {
            return otelResource;
        }

        public long getStartEpochNanos() {
            return startEpochNanos;
        }

        public long getEpochNanos() {
            return epochNanos;
        }
    }
}
