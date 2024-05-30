/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.providers;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import io.confluent.flink.table.modules.TestUtils.IncrementingClock;
import io.confluent.flink.table.modules.TestUtils.TrackingMetricsGroup;
import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import org.junit.jupiter.api.BeforeEach;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/** Test base for providers. */
public class ProviderTestBase {
    protected MLFunctionMetrics metrics;
    protected MetricGroup metricGroup;
    protected Clock clock = new IncrementingClock(Instant.now(), ZoneId.systemDefault());
    protected Map<String, Gauge<?>> registeredGauges = new HashMap<>();
    protected Map<String, Counter> registeredCounters = new HashMap<>();

    @BeforeEach
    void setUp() {
        clock = new IncrementingClock(Instant.now(), ZoneId.systemDefault());
        registeredGauges = new HashMap<>();
        registeredCounters = new HashMap<>();
        metricGroup = new TrackingMetricsGroup("m", registeredCounters, registeredGauges);
        metrics = new MLFunctionMetrics(metricGroup, MLFunctionMetrics.PREDICT_METRIC_NAME);
    }
}
