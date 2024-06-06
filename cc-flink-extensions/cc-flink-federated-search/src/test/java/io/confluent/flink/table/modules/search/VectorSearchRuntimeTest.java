/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.MockOkHttpClient;
import io.confluent.flink.table.utils.mlutils.MlUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for VectorSearchRuntime. */
public class VectorSearchRuntimeTest {
    MetricGroup metricGroup;
    MLFunctionMetrics metrics;
    Clock clock = new IncrementingClock(Instant.now(), ZoneId.systemDefault());
    MockOkHttpClient mockHttpClient = new MockOkHttpClient();
    Map<String, Gauge<?>> registeredGauges = new HashMap<>();
    Map<String, Counter> registeredCounters = new HashMap<>();
    Map<String, String> configuration;

    // TODO: Share the IncrementingClock and TrackingMetricsGroup from cc-flink-ml-functions module
    /** Mock clock for testing. */
    public static class IncrementingClock extends Clock {
        private Instant instant;
        private final ZoneId zone;

        public IncrementingClock(Instant fixedInstant, ZoneId zone) {
            this.instant = fixedInstant;
            this.zone = zone;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new IncrementingClock(instant, zone);
        }

        @Override
        public Instant instant() {
            instant = instant.plusMillis(1);
            return instant;
        }
    }

    /** Metrics Group to track metrics. */
    public static class TrackingMetricsGroup extends UnregisteredMetricsGroup {
        private final Map<String, Counter> counters;
        private final Map<String, Gauge<?>> gauges;
        private final String base;

        public TrackingMetricsGroup(
                String name, Map<String, Counter> counters, Map<String, Gauge<?>> gauges) {
            this.base = name;
            this.counters = counters;
            this.gauges = gauges;
        }

        @Override
        public Counter counter(String name) {
            Counter counter = new SimpleCounter();
            counters.put(base + "." + name, counter);
            return counter;
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            counters.put(base + "." + name, counter);
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(base + "." + name, gauge);
            return gauge;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return new TrackingMetricsGroup(base + "." + name, counters, gauges);
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return new TrackingMetricsGroup(base + "." + key + "." + value, counters, gauges);
        }
    }

    @BeforeEach
    void beforeTest() {
        registeredGauges.clear();
        registeredCounters.clear();
        metricGroup = new TrackingMetricsGroup("m", registeredCounters, registeredGauges);
        metrics = new MLFunctionMetrics(metricGroup, MLFunctionMetrics.VECTOR_SEARCH_METRIC_NAME);
        configuration =
                ImmutableMap.of(
                        MLModelCommonConstants.CREDENTIAL_SERVICE_HOST,
                        "localhost",
                        MLModelCommonConstants.CREDENTIAL_SERVICE_PORT,
                        "123",
                        MLModelCommonConstants.ORG_ID,
                        "org1",
                        MLModelCommonConstants.ENV_ID,
                        "env1",
                        MLModelCommonConstants.DATABASE_ID,
                        "db1",
                        MLModelCommonConstants.MODEL_NAME,
                        "model1",
                        MLModelCommonConstants.MODEL_VERSION,
                        "v1",
                        MLModelCommonConstants.COMPUTE_POOL_ID,
                        "cp1",
                        MLModelCommonConstants.ENCRYPT_STRATEGY,
                        "kms",
                        MLModelCommonConstants.COMPUTE_POOL_ENV_ID,
                        "cp_env1");
    }

    @Test
    void testRemoteHttpCallPinecone() throws Exception {
        CatalogTable table = getPineconeTable();
        VectorSearchRuntime runtime =
                VectorSearchRuntime.mockOpen(table, configuration, mockHttpClient, metrics, clock);
        mockHttpClient.withResponse(
                MlUtils.makeResponse(
                        "{\n"
                                + "  \"matches\":[\n"
                                + "    {\n"
                                + "      \"id\": \"vec3\",\n"
                                + "      \"score\": 0,\n"
                                + "      \"values\": [0.3,0.3,0.3,0.3],\n"
                                + "      \"metadata\": {\"key\": \"value1\", \"content\": \"content1\", \"content2\": 3}\n"
                                + "    },\n"
                                + "    {\n"
                                + "      \"id\": \"vec2\",\n"
                                + "      \"score\": 0.0800000429,\n"
                                + "      \"values\": [0.2, 0.2, 0.2, 0.2],\n"
                                + "      \"metadata\": {\"key\": \"value2\", \"content\": \"content2\", \"content2\": 4}\n"
                                + "    },\n"
                                + "    {\n"
                                + "      \"id\": \"vec4\",\n"
                                + "      \"score\": 0.0799999237,\n"
                                + "      \"values\": [0.4, 0.4, 0.4, 0.4],\n"
                                + "      \"metadata\": {\"key\": \"value3\", \"content\": \"content3\", \"content2\": 5}\n"
                                + "    }\n"
                                + "  ],\n"
                                + "  \"namespace\": \"example-namespace\",\n"
                                + "  \"usage\": {\"read_units\": 6}\n"
                                + "}"));
        Row results = runtime.run(new Object[] {3, new float[] {0.1f, 0.2f, 0.3f, 0.4f}});
        runtime.close();
        Assertions.assertThat(results.toString())
                .isEqualTo(
                        "+I[+I[content1, [0.3, 0.3, 0.3, 0.3]], +I[content2, [0.2, 0.2, 0.2, 0.2]], +I[content3, [0.4, 0.4, 0.4, 0.4]]]");
        Assertions.assertThat(registeredCounters.get("m.ConfluentVCTS.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(
                        registeredCounters.get("m.ConfluentVCTS.PINECONE.requests").getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredCounters.get("m.ConfluentVCTS.requestSuccesses").getCount())
                .isEqualTo(1);
        Assertions.assertThat(
                        registeredCounters
                                .get("m.ConfluentVCTS.PINECONE.requestSuccesses")
                                .getCount())
                .isEqualTo(1);
        Assertions.assertThat(registeredGauges.get("m.ConfluentVCTS.requestMs").getValue())
                .isEqualTo(1L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentVCTS.PINECONE.requestMs").getValue())
                .isEqualTo(1L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentVCTS.totalMs").getValue())
                .isEqualTo(3L);
        Assertions.assertThat(registeredGauges.get("m.ConfluentVCTS.PINECONE.totalMs").getValue())
                .isEqualTo(3L);

        Assertions.assertThat(registeredCounters.get("m.ConfluentVCTS.bytesSent").getCount())
                .isEqualTo(81);
        Assertions.assertThat(registeredCounters.get("m.ConfluentVCTS.bytesReceived").getCount())
                .isEqualTo(594);

        Assertions.assertThat(registeredCounters.get("m.ConfluentVCTS.request4XX").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentVCTS.request5XX").getCount())
                .isEqualTo(0);
        Assertions.assertThat(
                        registeredCounters
                                .get("m.ConfluentVCTS.requestOtherHttpFailures")
                                .getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentVCTS.parseFailures").getCount())
                .isEqualTo(0);
        Assertions.assertThat(registeredCounters.get("m.ConfluentVCTS.prepFailures").getCount())
                .isEqualTo(0);
        assertThat(registeredCounters.get("m.ConfluentVCTS.PLAINTEXT.requestSuccesses").getCount())
                .isEqualTo(1L);
        assertThat(registeredCounters.get("m.ConfluentVCTS.PLAINTEXT.requestFailures").getCount())
                .isEqualTo(0L);
        assertThat(registeredGauges.get("m.ConfluentVCTS.PLAINTEXT.requestMs").getValue())
                .isEqualTo(1L);
        assertThat(
                        registeredCounters
                                .get("m.ConfluentVCTS.PINECONE.PLAINTEXT.requestSuccesses")
                                .getCount())
                .isEqualTo(1L);
        assertThat(
                        registeredCounters
                                .get("m.ConfluentVCTS.PINECONE.PLAINTEXT.requestFailures")
                                .getCount())
                .isEqualTo(0L);
        assertThat(registeredGauges.get("m.ConfluentVCTS.PINECONE.PLAINTEXT.requestMs").getValue())
                .isEqualTo(1L);
    }

    CatalogTable getPineconeTable() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("pinecone.endpoint", "https://pinecone.com/not-yet-validated-endpoint");
        tableOptions.put("pinecone.api_key", "api-key");
        tableOptions.put("provider", "pinecone");
        return getTable(tableOptions);
    }

    CatalogTable getTable(Map<String, String> tableOptions) {
        // inputs
        Schema inputSchema =
                Schema.newBuilder()
                        .column("content", "STRING")
                        .column("embedding", "ARRAY<FLOAT>")
                        .build();
        ResolvedSchema resolvedInputSchema =
                ResolvedSchema.of(
                        Column.physical("content", DataTypes.STRING()),
                        Column.physical("embedding", DataTypes.ARRAY(DataTypes.FLOAT())));
        CatalogTable catalogTable =
                CatalogTable.of(inputSchema, "", Collections.emptyList(), tableOptions);
        ResolvedCatalogTable table = new ResolvedCatalogTable(catalogTable, resolvedInputSchema);
        return table;
    }
}
