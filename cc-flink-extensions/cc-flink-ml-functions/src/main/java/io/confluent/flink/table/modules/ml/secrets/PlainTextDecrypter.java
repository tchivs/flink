/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.secrets;

import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;

/** Plain text decrypter. */
public class PlainTextDecrypter<T> implements SecretDecrypter {
    private ModelOptionsUtils modelOptionsUtils;

    public PlainTextDecrypter(T model) {
        if (model instanceof CatalogModel) {
            modelOptionsUtils = new ModelOptionsUtils(((CatalogModel) model).getOptions());
        }
    }

    @Override
    public String decryptFromKey(String secretKey) {
        final String secret = modelOptionsUtils.getOption(secretKey);
        return secret == null ? "" : secret;
    }

    @Override
    public EncryptionStrategy supportedStrategy() {
        return EncryptionStrategy.PLAINTEXT;
    }

    @Override
    public String getProviderName() {
        return MLModelSupportedProviders.fromString(modelOptionsUtils.getProvider())
                .getProviderName();
    }
}
