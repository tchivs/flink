/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.confluent.flink.table.utils.ModelOptionsUtils;

/** Plain text decrypter. */
public class PlainTextDecrypter implements SecretDecrypter {
    private ModelOptionsUtils modelOptionsUtils;

    public PlainTextDecrypter(CatalogModel model) {
        modelOptionsUtils = new ModelOptionsUtils(model.getOptions());
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
}
