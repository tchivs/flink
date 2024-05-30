/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.secrets;

import io.confluent.flink.table.modules.ml.RemoteModelOptions;

/** Decrypt model secrets from provided key. */
public interface SecretDecrypter {

    /**
     * Decrypt from secret key in model options. Return empty string if secret not found in options.
     *
     * @param secretKey secret key
     * @return decrypted secret or empty string if key not found in options
     */
    String decryptFromKey(final String secretKey);

    RemoteModelOptions.EncryptionStrategy supportedStrategy();

    String getProviderName();
}
