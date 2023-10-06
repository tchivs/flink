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

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.table.service.ServiceTasks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_OTLP_FUNCTIONS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

class OtlpMetricsDataTableFunctionITCase extends AbstractTestBase {
    private static final int PARALLELISM = 4;
    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    private StreamExecutionEnvironment env;

    @BeforeEach
    public void before() {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.setParallelism(PARALLELISM);
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
                                ImmutableMap.of("resource.k8s.pod.name", "flink-0", "tag", "a"),
                                100L,
                                null),
                        Row.ofKind(
                                RowKind.INSERT,
                                t,
                                "test_counter",
                                ImmutableMap.of("resource.k8s.pod.name", "flink-0", "tag", "b"),
                                200L,
                                null));
    }
}
