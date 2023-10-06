/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.otlp;

import org.apache.flink.table.functions.ScalarFunction;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.resource.v1.Resource;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * OTLP_EXAMPLE_METRICS() function returns an example OTLP MetricsData protobuf payload, which can
 * be used to test OTLP_METRICS() table function.
 *
 * <p>This function is non-deterministic: the timestamps in the payload always reflect the current
 * wall-clock timestamp.
 */
public class OtlpExampleMetrics extends ScalarFunction {
    public static final String NAME = "OTLP_EXAMPLE_METRICS";

    public byte[] eval() {
        Instant t = Instant.now();
        MetricsData.Builder metricsDataBuilder = MetricsData.newBuilder();

        metricsDataBuilder
                .addResourceMetricsBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAttributes(
                                        KeyValue.newBuilder()
                                                .setKey("k8s.pod.name")
                                                .setValue(
                                                        AnyValue.newBuilder()
                                                                .setStringValue("flink-0"))))
                .addScopeMetricsBuilder()
                .addMetricsBuilder()
                .setName("test_counter")
                .setSum(
                        Sum.newBuilder()
                                .setAggregationTemporality(
                                        AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
                                .addDataPoints(
                                        NumberDataPoint.newBuilder()
                                                .setTimeUnixNano(
                                                        TimeUnit.SECONDS.toNanos(t.getEpochSecond())
                                                                + t.getNano())
                                                .setAsInt(100)
                                                .addAttributes(
                                                        KeyValue.newBuilder()
                                                                .setKey("tag")
                                                                .setValue(
                                                                        AnyValue.newBuilder()
                                                                                .setStringValue(
                                                                                        "a"))))
                                .addDataPoints(
                                        NumberDataPoint.newBuilder()
                                                .setTimeUnixNano(
                                                        TimeUnit.SECONDS.toNanos(t.getEpochSecond())
                                                                + t.getNano())
                                                .setAsInt(200)
                                                .addAttributes(
                                                        KeyValue.newBuilder()
                                                                .setKey("tag")
                                                                .setValue(
                                                                        AnyValue.newBuilder()
                                                                                .setStringValue(
                                                                                        "b")))));

        return metricsDataBuilder.build().toByteArray();
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
