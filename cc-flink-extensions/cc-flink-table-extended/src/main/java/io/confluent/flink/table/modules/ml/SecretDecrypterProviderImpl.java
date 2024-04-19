/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;

/** Implementation of {@link io.confluent.flink.table.modules.ml.SecretDecrypterProvider}. */
public class SecretDecrypterProviderImpl implements SecretDecrypterProvider {

    private final CatalogModel model;

    public SecretDecrypterProviderImpl(CatalogModel model) {
        this.model = model;
    }

    @Override
    public SecretDecrypter getDecrypter(String decryptStrategy) {
        if (decryptStrategy == null
                || decryptStrategy.equalsIgnoreCase(EncryptionStrategy.PLAINTEXT.name())) {
            return new PlainTextDecrypter(model);
        } else if (decryptStrategy.equalsIgnoreCase(EncryptionStrategy.KMS.name())) {
            return new FlinkCredentialServiceSecretDecrypter(model);
        }
        throw new IllegalArgumentException("Unsupported decrypte strategy: " + decryptStrategy);
    }
}
