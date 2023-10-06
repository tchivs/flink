/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.otlp;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.metrics.v1.DataPointFlags;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OTLP_METRICS table function converts OTLP MetricsData protobuf bytes into rows, flattening each
 * datapoint and its attributes into a single row.
 */
public class OtlpMetricsDataTableFunction extends TableFunction<Row> {
    public static final String NAME = "OTLP_METRICS";

    private final String resourceAttributePrefix = "resource.";
    private final String metricAttributePrefix = "";

    @DataTypeHint(
            "ROW<`timestamp` TIMESTAMP(9) WITH LOCAL TIME ZONE, metric STRING,"
                    + " attributes MAP<STRING, STRING>, value_int BIGINT, value_double DOUBLE>")
    public void eval(byte[] otlpBytes) {
        try {
            MetricsData metricsData = MetricsData.parseFrom(otlpBytes);
            metricsData.getResourceMetricsList().stream()
                    .flatMap(
                            resourceMetrics -> {
                                Map<String, Object> resourceAttributes =
                                        resourceMetrics.getResource().getAttributesList().stream()
                                                .collect(
                                                        HashMap::new,
                                                        (m, kv) -> {
                                                            Object value =
                                                                    parseAnyValue(kv.getValue());
                                                            if (value != null) {
                                                                m.put(
                                                                        resourceAttributePrefix
                                                                                + kv.getKey(),
                                                                        value);
                                                            }
                                                        },
                                                        HashMap::putAll);
                                return resourceMetrics.getScopeMetricsList().stream()
                                        .flatMap(
                                                scopeMetrics ->
                                                        scopeMetrics.getMetricsList().stream()
                                                                .flatMap(
                                                                        metric ->
                                                                                parseMetric(
                                                                                        metric,
                                                                                        resourceAttributes)
                                                                                        .stream()));
                            })
                    .forEach(this::collect);

        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to parse OTLP metrics payload", e);
        }
    }

    private List<Row> parseMetric(Metric metric, Map<String, Object> resourceAttributes) {
        final List<Row> inputRows;
        String metricName = metric.getName();
        switch (metric.getDataCase()) {
            case SUM:
                {
                    inputRows = new ArrayList<>(metric.getSum().getDataPointsCount());
                    metric.getSum()
                            .getDataPointsList()
                            .forEach(
                                    dataPoint -> {
                                        if (hasRecordedValue(dataPoint)) {
                                            inputRows.add(
                                                    parseNumberDataPoint(
                                                            dataPoint,
                                                            resourceAttributes,
                                                            metricName));
                                        }
                                    });
                    break;
                }
            case GAUGE:
                {
                    inputRows = new ArrayList<>(metric.getGauge().getDataPointsCount());
                    metric.getGauge()
                            .getDataPointsList()
                            .forEach(
                                    dataPoint -> {
                                        if (hasRecordedValue(dataPoint)) {
                                            inputRows.add(
                                                    parseNumberDataPoint(
                                                            dataPoint,
                                                            resourceAttributes,
                                                            metricName));
                                        }
                                    });
                    break;
                }
                // TODO Support HISTOGRAM and SUMMARY metrics
            case HISTOGRAM:
            case SUMMARY:
            default:
                inputRows = Collections.emptyList();
        }
        return inputRows;
    }

    private static boolean hasRecordedValue(NumberDataPoint d) {
        return (d.getFlags() & DataPointFlags.DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK.getNumber())
                == 0;
    }

    private Row parseNumberDataPoint(
            NumberDataPoint dataPoint, Map<String, Object> resourceAttributes, String metricName) {
        HashMap<String, Object> attributes =
                new HashMap<>(dataPoint.getAttributesCount() + resourceAttributes.size());
        attributes.putAll(resourceAttributes);
        dataPoint
                .getAttributesList()
                .forEach(
                        att -> {
                            Object value = parseAnyValue(att.getValue());
                            if (value != null) {
                                // convert all attributes to string to fit the current data type
                                // constraints
                                attributes.put(
                                        metricAttributePrefix + att.getKey(), value.toString());
                            }
                        });

        return Row.of(
                Instant.ofEpochSecond(0, dataPoint.getTimeUnixNano()),
                metricName,
                attributes,
                dataPoint.hasAsInt() ? dataPoint.getAsInt() : null,
                dataPoint.hasAsDouble() ? dataPoint.getAsDouble() : null);
    }

    @Nullable
    private static Object parseAnyValue(AnyValue value) {
        switch (value.getValueCase()) {
            case INT_VALUE:
                return value.getIntValue();
            case BOOL_VALUE:
                return value.getBoolValue();
            case DOUBLE_VALUE:
                return value.getDoubleValue();
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
