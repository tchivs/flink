/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;

import io.confluent.flink.credentials.FileCredentialDecrypterImpl;

/** Mock for {@link FileCredentialDecrypterImpl}. */
@Confluent
public class MockFileCredentialDecrypterImpl extends FileCredentialDecrypterImpl {

    private byte[] keyBytes;

    public MockFileCredentialDecrypterImpl withPrivateKeyBytes(byte[] keyBytes) {
        this.keyBytes = keyBytes;
        return this;
    }

    @Override
    public void init(Configuration configuration) {}

    @Override
    public byte[] getPrivateKey() {
        return keyBytes;
    }
}
