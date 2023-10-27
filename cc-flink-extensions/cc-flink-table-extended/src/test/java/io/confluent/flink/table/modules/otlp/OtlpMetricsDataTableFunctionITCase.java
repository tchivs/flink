/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.otlp;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import io.confluent.flink.table.service.ServiceTasks;
import io.opentelemetry.proto.common.v1.AnyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_OTLP_FUNCTIONS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

class OtlpMetricsDataTableFunctionITCase extends AbstractTestBase {

    private static final Map<String, String> IMPLICIT_RESOURCE_ATTRIBUTES =
            Collections.unmodifiableMap(
                    OtlpExampleMetrics.RESOURCE_ATTRIBUTES.entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            e -> String.format("resource.%s", e.getKey()),
                                            e -> asString(e.getValue()))));

    private static final int PARALLELISM = 4;
    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    private static String asString(AnyValue any) {
        switch (any.getValueCase()) {
            case INT_VALUE:
                return String.valueOf(any.getIntValue());
            case DOUBLE_VALUE:
                return String.valueOf(any.getDoubleValue());
            case BOOL_VALUE:
                return String.valueOf(any.getBoolValue());
            case STRING_VALUE:
                return any.getStringValue();
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported value type [%s].", any.getValueCase()));
        }
    }

    private static Map<String, String> withAdditionalAttribute(String key, String value) {
        final Map<String, String> copy = new HashMap<>(IMPLICIT_RESOURCE_ATTRIBUTES);
        copy.put(key, value);
        return Collections.unmodifiableMap(copy);
    }

    @BeforeEach
    public void before() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
    }

    private static TableEnvironment getSqlServiceTableEnvironment() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.singletonMap(
                        CONFLUENT_OTLP_FUNCTIONS_ENABLED.key(), String.valueOf(true)),
                ServiceTasks.Service.SQL_SERVICE);
        return tableEnv;
    }

    @Test
    void testMetricsAggregation() {
        final TableEnvironment tEnv = getSqlServiceTableEnvironment();

        final TableResult result =
                tEnv.executeSql(
                        "SELECT `timestamp`, metric, SUM(value_int) FROM LATERAL TABLE("
                                + "OTLP_METRICS( OTLP_EXAMPLE_METRICS() )"
                                + ")  GROUP BY `timestamp`, metric");
        final List<Row> results = new ArrayList<>();
        result.collect().forEachRemaining(results::add);

        Instant t = (Instant) results.get(0).getField("timestamp");
        assertThat(results)
                .containsExactly(
                        Row.ofKind(RowKind.INSERT, t, "test_counter", 100L),
                        Row.ofKind(RowKind.UPDATE_BEFORE, t, "test_counter", 100L),
                        Row.ofKind(RowKind.UPDATE_AFTER, t, "test_counter", 300L));
    }

    @Test
    void testMetric() {
        final TableEnvironment tEnv = getSqlServiceTableEnvironment();

        final TableResult result =
                tEnv.executeSql(
                        "SELECT * FROM LATERAL TABLE ("
                                + "OTLP_METRICS( OTLP_EXAMPLE_METRICS() )"
                                + " )");
        final List<Row> results = new ArrayList<>();
        result.collect().forEachRemaining(results::add);

        Instant t = (Instant) results.get(0).getField("timestamp");
        assertThat(results)
                .containsExactly(
                        Row.ofKind(
                                RowKind.INSERT,
                                t,
                                "test_counter",
                                withAdditionalAttribute("tag", "a"),
                                100L,
                                null),
                        Row.ofKind(
                                RowKind.INSERT,
                                t,
                                "test_counter",
                                withAdditionalAttribute("tag", "b"),
                                200L,
                                null));
    }
}
