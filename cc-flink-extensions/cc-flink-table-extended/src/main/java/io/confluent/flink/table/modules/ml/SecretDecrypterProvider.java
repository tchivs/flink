/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

/**
 * Provider to select {@link io.confluent.flink.table.modules.ml.SecretDecrypter} based strategy.
 */
public interface SecretDecrypterProvider {
    SecretDecrypter getDecrypter(final String decryptStrategy);

    MeteredSecretDecrypter getMeteredDecrypter(final String decryptStrategy);
}
