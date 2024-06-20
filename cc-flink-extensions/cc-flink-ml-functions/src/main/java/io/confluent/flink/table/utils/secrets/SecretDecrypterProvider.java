/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.secrets;

/** Provider to select {@link SecretDecrypter} based strategy. */
public interface SecretDecrypterProvider {
    SecretDecrypter getDecrypter(final String decryptStrategy);

    MeteredSecretDecrypter getMeteredDecrypter(final String decryptStrategy);
}
