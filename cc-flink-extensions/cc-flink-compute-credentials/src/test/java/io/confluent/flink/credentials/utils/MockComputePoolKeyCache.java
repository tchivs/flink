/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials.utils;

import org.apache.flink.configuration.Configuration;

import io.confluent.flink.credentials.ComputePoolKeyCache;

import java.util.Optional;

/** Mocks a {@link ComputePoolKeyCache}. */
public class MockComputePoolKeyCache implements ComputePoolKeyCache {

    private byte[] computePoolKey;

    @Override
    public void init(Configuration configuration) {}

    @Override
    public void onNewPrivateKeyObtained(byte[] computePoolKey) {
        this.computePoolKey = computePoolKey;
    }

    @Override
    public Optional<byte[]> getPrivateKey() {
        return Optional.ofNullable(computePoolKey);
    }
}
