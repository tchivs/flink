/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.secrets;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.table.modules.TestUtils.TrackingMetricsGroup;
import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for SecretDecrypterProviderImpl. */
public class SecretDecrypterProviderImplTest {
    @Test
    public void shouldReturnCorrectDecrypter() {
        MLFunctionMetrics metrics =
                new MLFunctionMetrics(
                        new TrackingMetricsGroup("m", new HashMap<>(), new HashMap<>()),
                        MLFunctionMetrics.PREDICT_METRIC_NAME);

        Map<String, String> immutableMap =
                ImmutableMap.of(
                        "provider",
                        "openai",
                        "openai.api_key",
                        "api_key",
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
                        "cp1");

        Map<String, String> options = new HashMap<>(immutableMap);
        options.put(MLModelCommonConstants.ENCRYPT_STRATEGY, "kms");
        options.put(MLModelCommonConstants.COMPUTE_POOL_ENV_ID, "cp_env1");

        CatalogModel model =
                CatalogModel.of(
                        Schema.newBuilder().build(), Schema.newBuilder().build(), options, null);
        assertThat(
                        new SecretDecrypterProviderImpl<>(model, ImmutableMap.of(), metrics)
                                .getDecrypter("plaintext"))
                .isInstanceOf(PlainTextDecrypter.class);
        assertThat(
                        new SecretDecrypterProviderImpl<>(model, ImmutableMap.of(), metrics)
                                .getDecrypter("kms"))
                .isInstanceOf(FlinkCredentialServiceSecretDecrypter.class);
        assertThatThrownBy(
                        () ->
                                new SecretDecrypterProviderImpl<>(model, ImmutableMap.of(), metrics)
                                        .getDecrypter("invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported decrypte strategy: invalid");

        assertThat(
                        new SecretDecrypterProviderImpl<>(model, ImmutableMap.of(), metrics)
                                .getMeteredDecrypter("plaintext"))
                .isInstanceOf(MeteredSecretDecrypter.class);
        assertThat(
                        new SecretDecrypterProviderImpl<>(model, ImmutableMap.of(), metrics)
                                .getMeteredDecrypter("kms"))
                .isInstanceOf(MeteredSecretDecrypter.class);
    }
}
