/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.otlp;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.metrics.v1.DataPointFlags;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * OTLP_METRICS table function converts OTLP MetricsData protobuf bytes into rows, flattening each
 * datapoint and its attributes into a single row.
 */
public class OtlpMetricsDataTableFunction extends TableFunction<Row> {

    static final String NAME = "OTLP_METRICS";

    private static final String RESOURCE_ATTR_PREFIX = "resource.";
    private static final String METRIC_ATTR_PREFIX = "";

    @DataTypeHint(
            "ROW<`timestamp` TIMESTAMP(9) WITH LOCAL TIME ZONE, metric STRING,"
                    + " attributes MAP<STRING, STRING>, value_int BIGINT, value_double DOUBLE>")
    public void eval(byte[] otlpBytes) {
        try {
            doEval(MetricsData.parseFrom(otlpBytes).getResourceMetricsList());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse OTLP metrics payload", e);
        }
    }

    private void doEval(List<ResourceMetrics> resourceMetrics) {
        // Since we explode all bundled metrics to rows, we need to attach resource attributes to
        // each row. We also need to work around some data type limitations by converting all
        // attributes to string.
        final Map<String, String> resourceAttributes =
                resourceMetrics.stream()
                        .map(ResourceMetrics::getResource)
                        .map(Resource::getAttributesList)
                        .flatMap(Collection::stream)
                        .map(kv -> Tuple2.of(kv.getKey(), anyAsString(kv.getValue())))
                        .filter(kv -> kv.f1 != null)
                        .collect(Collectors.toMap(t -> RESOURCE_ATTR_PREFIX + t.f0, t -> t.f1));

        resourceMetrics.stream()
                .map(ResourceMetrics::getScopeMetricsList)
                .flatMap(Collection::stream)
                .map(ScopeMetrics::getMetricsList)
                .flatMap(Collection::stream)
                .flatMap(metric -> explodeMetricToRows(metric, resourceAttributes))
                .forEach(this::collect);
    }

    private static Stream<Row> explodeMetricToRows(
            Metric metric, Map<String, String> resourceAttributes) {
        final String metricName = metric.getName();
        switch (metric.getDataCase()) {
            case SUM:
                return asRowStream(
                        metric.getSum().getDataPointsList(), resourceAttributes, metricName);
            case GAUGE:
                return asRowStream(
                        metric.getGauge().getDataPointsList(), resourceAttributes, metricName);
            case HISTOGRAM:
            case SUMMARY:
                // TODO Support HISTOGRAM and SUMMARY metrics
            default:
                return Stream.empty();
        }
    }

    private static Stream<Row> asRowStream(
            List<NumberDataPoint> dataPoints,
            Map<String, String> resourceAttributes,
            String metricName) {
        return dataPoints.stream()
                .filter(OtlpMetricsDataTableFunction::hasRecordedValue)
                .map(dataPoint -> asRow(dataPoint, resourceAttributes, metricName));
    }

    private static Row asRow(
            NumberDataPoint dataPoint, Map<String, String> resourceAttributes, String metricName) {
        final HashMap<String, Object> attributes =
                new HashMap<>(dataPoint.getAttributesCount() + resourceAttributes.size());
        attributes.putAll(resourceAttributes);
        dataPoint
                .getAttributesList()
                .forEach(
                        att -> {
                            final String value = anyAsString(att.getValue());
                            if (value != null) {
                                // convert all attributes to string to fit the current data type
                                // constraints
                                attributes.put(METRIC_ATTR_PREFIX + att.getKey(), value);
                            }
                        });

        return Row.of(
                Instant.ofEpochSecond(0, dataPoint.getTimeUnixNano()),
                metricName,
                attributes,
                dataPoint.hasAsInt() ? dataPoint.getAsInt() : null,
                dataPoint.hasAsDouble() ? dataPoint.getAsDouble() : null);
    }

    private static boolean hasRecordedValue(NumberDataPoint d) {
        return (d.getFlags() & DataPointFlags.DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK.getNumber())
                == 0;
    }

    @Nullable
    private static String anyAsString(AnyValue value) {
        switch (value.getValueCase()) {
            case INT_VALUE:
                return String.valueOf(value.getIntValue());
            case BOOL_VALUE:
                return String.valueOf(value.getBoolValue());
            case DOUBLE_VALUE:
                return String.valueOf(value.getDoubleValue());
            case STRING_VALUE:
                return value.getStringValue();
            case ARRAY_VALUE:
            case KVLIST_VALUE:
            case BYTES_VALUE:
                // TODO: complex types are not currently supported due to static SQL type
                // constraints
            case VALUE_NOT_SET:
            default:
                return null;
        }
    }
}
