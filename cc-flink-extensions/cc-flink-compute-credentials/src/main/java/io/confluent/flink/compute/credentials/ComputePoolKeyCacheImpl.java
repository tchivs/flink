/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.compute.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Optional;

/**
 * Used to cache the compute pool private key. There must be a periodic service which fetches and
 * sets the compute pool key on this cache with {@code onNewPrivateKeyObtained}. The cache allows
 * calls to {@code getPrivateKey} enabling callers to wait until compute pool key arrives.
 */
@Confluent
public class ComputePoolKeyCacheImpl implements ComputePoolKeyCache {
    private static final Logger LOG = LoggerFactory.getLogger(ComputePoolKeyCacheImpl.class);

    @GuardedBy("this")
    private byte[] computePoolKey;

    @GuardedBy("this")
    private Configuration configuration = new Configuration();

    public static final ComputePoolKeyCacheImpl INSTANCE = new ComputePoolKeyCacheImpl();

    public synchronized void init(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Called when a new compute pool key is available.
     *
     * @param computePoolKey The serialized compute pool key.
     */
    @Override
    public synchronized void onNewPrivateKeyObtained(byte[] computePoolKey) {
        LOG.info("Compute pool key has been obtained");
        this.computePoolKey = computePoolKey;
        notifyAll();
    }

    /**
     * Gets the key for the given compute pool, possibly blocking until it is available or a timeout
     * occurs.
     *
     * @return The compute pool private key when it is available or empty if a timeout occurs.
     */
    @Override
    public Optional<byte[]> getPrivateKey() {
        return getPrivateKey(SystemClock.getInstance());
    }

    @VisibleForTesting
    synchronized Optional<byte[]> getPrivateKey(Clock clock) {
        long timeUpMs =
                clock.absoluteTimeMillis()
                        + configuration.getLong(ComputePoolKeyCacheOptions.RECEIVE_TIMEOUT_MS);
        long remaining;
        while (computePoolKey == null && (remaining = timeUpMs - clock.absoluteTimeMillis()) > 0) {
            try {
                wait(remaining);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException("Error retrieving compute pool key", e);
            }
        }
        if (computePoolKey != null) {
            return Optional.of(computePoolKey);
        } else {
            if (configuration.getBoolean(ComputePoolKeyCacheOptions.RECEIVE_ERROR_ON_TIMEOUT)) {
                LOG.error("Timed out waiting for compute pool key");
                throw new FlinkRuntimeException("Timed out waiting for compute pool key");
            } else {
                return Optional.empty();
            }
        }
    }
}
