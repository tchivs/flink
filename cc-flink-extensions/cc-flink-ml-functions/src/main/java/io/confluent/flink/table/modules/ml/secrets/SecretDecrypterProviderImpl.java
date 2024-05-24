/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.secrets;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;

import java.time.Clock;

/** Implementation of {@link SecretDecrypterProvider}. */
public class SecretDecrypterProviderImpl implements SecretDecrypterProvider {

    private final CatalogModel model;
    private final MLFunctionMetrics metrics;
    private final Clock clock;

    public SecretDecrypterProviderImpl(CatalogModel model, MLFunctionMetrics metrics) {
        this(model, metrics, Clock.systemUTC());
    }

    @VisibleForTesting
    public SecretDecrypterProviderImpl(CatalogModel model, MLFunctionMetrics metrics, Clock clock) {
        this.model = model;
        this.metrics = metrics;
        this.clock = clock;
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

    @Override
    public MeteredSecretDecrypter getMeteredDecrypter(String decryptStrategy) {
        return new MeteredSecretDecrypter(getDecrypter(decryptStrategy), metrics, clock);
    }
}
