/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;

import io.confluent.flink.credentials.CredentialDecrypter;

/** Mock for {@link CredentialDecrypter}. */
@Confluent
public class MockCredentialDecrypter implements CredentialDecrypter {

    private byte[] result;
    private boolean error;

    public MockCredentialDecrypter withDecryptedResult(byte[] result) {
        this.result = result;
        return this;
    }

    public MockCredentialDecrypter withError() {
        this.error = true;
        return this;
    }

    public void init(Configuration configuration) {}

    public byte[] decrypt(byte[] value) {
        if (error) {
            throw new RuntimeException("Decryption Error");
        }
        return result;
    }
}
