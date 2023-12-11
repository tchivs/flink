/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.compute.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.clock.ManualClock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link io.confluent.flink.compute.credentials.ComputePoolKeyCache}. */
@Confluent
public class ComputePoolKeyCacheImplTest {

    private byte[] computePoolKey;
    private Configuration configuration;
    private ExecutorService executor;

    private final ComputePoolKeyCacheImpl computePoolKeyCache = ComputePoolKeyCacheImpl.INSTANCE;

    @Before
    public void setUp() {
        // Clear the cache before each test
        computePoolKeyCache.onNewPrivateKeyObtained(null);
        computePoolKey = "computePoolKey".getBytes(StandardCharsets.UTF_8);

        configuration = new Configuration();
        configuration.setLong(ComputePoolKeyCacheOptions.RECEIVE_TIMEOUT_MS, 50);

        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testReceive_one() {
        computePoolKeyCache.onNewPrivateKeyObtained(computePoolKey);
        Optional<byte[]> result = computePoolKeyCache.getPrivateKey();

        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(computePoolKey);
    }

    @Test
    public void testReceive_waitUntilDataArrives() {
        computePoolKeyCache.init(configuration);
        executor.submit(
                () -> {
                    try {
                        Thread.sleep(10);
                        computePoolKeyCache.onNewPrivateKeyObtained(computePoolKey);
                    } catch (Exception e) {
                        fail("Got exception", e);
                    }
                });
        Optional<byte[]> result = computePoolKeyCache.getPrivateKey(new ManualClock());

        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(computePoolKey);
    }

    @Test
    public void testReceive_interrupted() throws Throwable {
        final Object lock = new Object();

        computePoolKeyCache.init(configuration);
        AtomicReference<Thread> thread = new AtomicReference<>();
        AtomicBoolean isInterrupted = new AtomicBoolean(false);
        AtomicBoolean isDone = new AtomicBoolean(false);

        executor.submit(
                () -> {
                    try {
                        thread.set(Thread.currentThread());
                        synchronized (lock) {
                            lock.notifyAll();
                        }
                        computePoolKeyCache.getPrivateKey(new ManualClock());
                    } catch (Exception e) {
                        isInterrupted.set(Thread.currentThread().isInterrupted());
                        return;
                    } finally {
                        synchronized (lock) {
                            isDone.set(true);
                            lock.notifyAll();
                        }
                    }
                    fail("Shouldn't get to here");
                });

        synchronized (lock) {
            while (thread.get() == null) {
                lock.wait();
            }
        }

        thread.get().interrupt();

        synchronized (lock) {
            while (!isDone.get()) {
                lock.wait();
            }
        }

        assertThat(isInterrupted.get()).isTrue();
    }

    @Test
    public void testReceive_neverArrives() {
        computePoolKeyCache.init(configuration);
        assertThatThrownBy(computePoolKeyCache::getPrivateKey)
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage("Timed out waiting for compute pool key");
    }

    @Test
    public void testReceive_neverArrives_noError() {
        configuration.setBoolean(ComputePoolKeyCacheOptions.RECEIVE_ERROR_ON_TIMEOUT, false);
        computePoolKeyCache.init(configuration);
        Optional<byte[]> result = computePoolKeyCache.getPrivateKey();
        assertThat(result.isPresent()).isFalse();
    }
}
