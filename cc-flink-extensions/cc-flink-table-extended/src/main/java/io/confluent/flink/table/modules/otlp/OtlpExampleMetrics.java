/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.otlp;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.functions.ScalarFunction;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Sum;
import io.opentelemetry.proto.resource.v1.Resource;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * OTLP_EXAMPLE_METRICS() function returns an example OTLP MetricsData protobuf payload, which can
 * be used to test OTLP_METRICS() table function.
 *
 * <p>This function is non-deterministic: the timestamps in the payload always reflect the current
 * wall-clock timestamp.
 */
public class OtlpExampleMetrics extends ScalarFunction {

    static final String NAME = "OTLP_EXAMPLE_METRICS";

    @VisibleForTesting static final Map<String, AnyValue> RESOURCE_ATTRIBUTES;

    static {
        final Map<String, AnyValue> resourceAttributes = new HashMap<>();
        resourceAttributes.put("string-attr", AnyValue.newBuilder().setStringValue("foo").build());
        resourceAttributes.put("long-attr", AnyValue.newBuilder().setIntValue(1337).build());
        resourceAttributes.put("bool-attr", AnyValue.newBuilder().setBoolValue(true).build());
        resourceAttributes.put("double-attr", AnyValue.newBuilder().setDoubleValue(13.37).build());
        RESOURCE_ATTRIBUTES = Collections.unmodifiableMap(resourceAttributes);
    }

    public byte[] eval() {
        Instant t = Instant.now();
        MetricsData.Builder metricsDataBuilder = MetricsData.newBuilder();
        metricsDataBuilder
                .addResourceMetricsBuilder()
                .setResource(
                        Resource.newBuilder()
                                .addAllAttributes(
                                        RESOURCE_ATTRIBUTES.entrySet().stream()
                                                .map(
                                                        e ->
                                                                KeyValue.newBuilder()
                                                                        .setKey(e.getKey())
                                                                        .setValue(e.getValue())
                                                                        .build())
                                                .collect(Collectors.toList())))
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
