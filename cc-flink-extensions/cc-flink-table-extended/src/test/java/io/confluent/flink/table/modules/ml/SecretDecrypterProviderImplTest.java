/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.table.modules.TestUtils.TrackingMetricsGroup;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_HOST;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for SecretDecrypterProviderImpl. */
public class SecretDecrypterProviderImplTest {
    @Test
    public void shouldReturnCorrectDecrypter() {
        MLFunctionMetrics metrics =
                new MLFunctionMetrics(
                        new TrackingMetricsGroup("m", new HashMap<>(), new HashMap<>()));

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
        assertThat(new SecretDecrypterProviderImpl(model, metrics).getDecrypter("plaintext"))
                .isInstanceOf(PlainTextDecrypter.class);
        assertThat(new SecretDecrypterProviderImpl(model, metrics).getDecrypter("kms"))
                .isInstanceOf(FlinkCredentialServiceSecretDecrypter.class);
        assertThatThrownBy(
                        () ->
                                new SecretDecrypterProviderImpl(model, metrics)
                                        .getDecrypter("invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported decrypte strategy: invalid");

        assertThat(new SecretDecrypterProviderImpl(model, metrics).getMeteredDecrypter("plaintext"))
                .isInstanceOf(MeteredSecretDecrypter.class);
        assertThat(new SecretDecrypterProviderImpl(model, metrics).getMeteredDecrypter("kms"))
                .isInstanceOf(MeteredSecretDecrypter.class);
    }
}
