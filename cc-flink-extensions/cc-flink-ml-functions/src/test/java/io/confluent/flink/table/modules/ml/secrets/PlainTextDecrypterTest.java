/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.secrets;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.table.modules.TestUtils.TrackingMetricsGroup;
import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_HOST;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_PORT;
import static org.assertj.core.api.Assertions.assertThat;

/** Test PlainTextDecrypter. */
public class PlainTextDecrypterTest {

    @Test
    public void testDecrypt() {
        CatalogModel model =
                CatalogModel.of(
                        Schema.newBuilder().build(),
                        Schema.newBuilder().build(),
                        ImmutableMap.of(
                                "provider",
                                "openai",
                                "openai.api_key",
                                "api_key",
                                CREDENTIAL_SERVICE_HOST.key(),
                                "localhost",
                                CREDENTIAL_SERVICE_PORT.key(),
                                "1234"),
                        null);
        MLFunctionMetrics metrics =
                new MLFunctionMetrics(
                        new TrackingMetricsGroup("m", new HashMap<>(), new HashMap<>()),
                        MLFunctionMetrics.PREDICT_METRIC_NAME);
        PlainTextDecrypter decrypter = new PlainTextDecrypter(model);
        assertThat(decrypter.decryptFromKey("some_key")).isEmpty();
        assertThat(decrypter.decryptFromKey("openai.api_key")).isEqualTo("api_key");
        assertThat(decrypter.supportedStrategy()).isEqualTo(EncryptionStrategy.PLAINTEXT);
    }
}
