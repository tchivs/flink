/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_HOST;
import static io.confluent.flink.credentials.KafkaCredentialsOptions.CREDENTIAL_SERVICE_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for FlinkCredentialServiceSecretDecrypter. */
public class FlinkCredentialServiceSecretDecrypterTest {
    private CatalogModel model;
    private Map<String, String> options;
    private FlinkCredentialServiceSecretDecrypter decrypter;

    @BeforeEach
    void setUp() {
        Map<String, String> immutableMap =
                ImmutableMap.of(
                        "provider",
                        "openai",
                        "openai.api_key",
                        "api_key",
                        CREDENTIAL_SERVICE_HOST.key(),
                        "localhost",
                        CREDENTIAL_SERVICE_PORT.key(),
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
        options = new HashMap<>(immutableMap);
        options.put(MLModelCommonConstants.ENCRYPT_STRATEGY, "kms");
        model = modelFromOption(options);
        decrypter = new FlinkCredentialServiceSecretDecrypter(model);
    }

    @Test
    public void testStrategy() {
        assertThat(decrypter.supportedStrategy()).isEqualTo(EncryptionStrategy.KMS);
    }

    @Test
    public void testStrategyMatch() {
        options.remove(MLModelCommonConstants.ENCRYPT_STRATEGY);
        model = modelFromOption(options);
        decrypter = new FlinkCredentialServiceSecretDecrypter(model);
        assertThatThrownBy(() -> decrypter.decryptFromKey("openai.api_key"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Decrypt key which is not KMS encrypted");
    }

    @Test
    public void testNonExistKey() {
        assertThat(decrypter.decryptFromKey("somekey")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                MLModelCommonConstants.ORG_ID,
                MLModelCommonConstants.ENV_ID,
                MLModelCommonConstants.DATABASE_ID,
                MLModelCommonConstants.COMPUTE_POOL_ID,
                MLModelCommonConstants.MODEL_VERSION,
                MLModelCommonConstants.MODEL_NAME
            })
    public void missParam(String param) {
        options.remove(param);
        model = modelFromOption(options);
        decrypter = new FlinkCredentialServiceSecretDecrypter(model);
        assertThatThrownBy(() -> decrypter.decryptFromKey("openai.api_key"))
                .isInstanceOf(NullPointerException.class);
    }

    private static CatalogModel modelFromOption(Map<String, String> options) {
        return CatalogModel.of(
                Schema.newBuilder().build(), Schema.newBuilder().build(), options, null);
    }
}
