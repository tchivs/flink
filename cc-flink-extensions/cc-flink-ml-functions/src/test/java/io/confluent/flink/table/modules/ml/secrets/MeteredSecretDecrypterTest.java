/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.secrets;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import io.confluent.flink.table.modules.TestUtils.IncrementingClock;
import io.confluent.flink.table.modules.TestUtils.MockFlinkCredentialServiceDecrypter;
import io.confluent.flink.table.modules.TestUtils.TrackingMetricsGroup;
import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.providers.AzureMLProviderTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for MeteredSecretDecrypter. */
public class MeteredSecretDecrypterTest {

    @ParameterizedTest(name = "hasFailure={0}")
    @ValueSource(booleans = {true, false})
    public void testMetrics(boolean shouldThrow) {
        Clock clock = new IncrementingClock(Instant.now(), ZoneId.systemDefault());
        Map<String, Gauge<?>> registeredGauges = new HashMap<>();
        Map<String, Counter> registeredCounters = new HashMap<>();
        MetricGroup metricGroup =
                new TrackingMetricsGroup("m", registeredCounters, registeredGauges);
        MLFunctionMetrics metrics = new MLFunctionMetrics(metricGroup);

        MeteredSecretDecrypter meteredSecretDecrypter =
                new MeteredSecretDecrypter(
                        new MockFlinkCredentialServiceDecrypter(
                                AzureMLProviderTest.getCatalogModel(), shouldThrow),
                        metrics,
                        clock);
        try {
            meteredSecretDecrypter.decryptFromKey("azureml.api_key");
        } catch (Exception e) {
            // Do nothing
        }

        assertThat(registeredGauges.get("m.ConfluentML.AZUREML.KMS.requestMs").getValue())
                .isEqualTo(1L);

        long successCount = shouldThrow ? 0L : 1L;
        long failureCount = shouldThrow ? 1L : 0L;
        assertThat(registeredCounters.get("m.ConfluentML.AZUREML.KMS.requestSuccesses").getCount())
                .isEqualTo(successCount);
        assertThat(registeredCounters.get("m.ConfluentML.AZUREML.KMS.requestFailures").getCount())
                .isEqualTo(failureCount);
    }
}
