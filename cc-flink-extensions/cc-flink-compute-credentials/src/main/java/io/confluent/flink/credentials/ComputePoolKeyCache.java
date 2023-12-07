/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

/**
 * Used to cache the compute pool private key. There must be a periodic service which fetches and
 * sets the compute pool key on this cache with {@code onNewPrivateKeyObtained}. The cache allows
 * calls to {@code getPrivateKey} enabling callers to wait until compute pool key arrives.
 */
@Confluent
public interface ComputePoolKeyCache {

    /**
     * Initializes the cache.
     *
     * @param configuration The Flink Configuration
     */
    void init(Configuration configuration);

    /**
     * Called when a new compute pool key is available.
     *
     * @param computePoolKey The serialized compute pool key.
     */
    void onNewPrivateKeyObtained(byte[] computePoolKey);

    /**
     * Gets the key for the given compute pool, possibly blocking until it is available or a timeout
     * occurs.
     *
     * @return The compute pool private key when it is available or empty if a timeout occurs.
     */
    Optional<byte[]> getPrivateKey();
}
