/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.secrets;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;

import java.time.Clock;
import java.util.Map;

/** Implementation of {@link SecretDecrypterProvider}. */
public class SecretDecrypterProviderImpl<T> implements SecretDecrypterProvider {

    private final T resource;
    private final MLFunctionMetrics metrics;
    private final Clock clock;
    private final Map<String, String> configuration;

    public SecretDecrypterProviderImpl(
            T resource, Map<String, String> configuration, MLFunctionMetrics metrics) {
        this(resource, configuration, metrics, Clock.systemUTC());
    }

    @VisibleForTesting
    public SecretDecrypterProviderImpl(
            T resource, Map<String, String> configuration, MLFunctionMetrics metrics, Clock clock) {
        this.resource = resource;
        this.configuration = configuration;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public SecretDecrypter getDecrypter(String decryptStrategy) {
        if (decryptStrategy == null
                || decryptStrategy.equalsIgnoreCase(EncryptionStrategy.PLAINTEXT.name())) {
            return new PlainTextDecrypter(resource);
        } else if (decryptStrategy.equalsIgnoreCase(EncryptionStrategy.KMS.name())) {
            if (resource instanceof CatalogModel) {
                return new ModelSecretDecrypter((CatalogModel) resource, configuration);
            } else {
                // TODO: support table
                throw new UnsupportedOperationException(
                        "Not supported resource type for decrypter: "
                                + resource.getClass().getSimpleName());
            }
        }
        throw new IllegalArgumentException("Unsupported decrypte strategy: " + decryptStrategy);
    }

    @Override
    public MeteredSecretDecrypter getMeteredDecrypter(String decryptStrategy) {
        return new MeteredSecretDecrypter(getDecrypter(decryptStrategy), metrics, clock);
    }
}
