/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token.kafka;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.clock.ManualClock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link KafkaCredentialsCacheImpl}. */
@Confluent
public class KafkaCredentialsCacheImplTest {

    private Properties credProperties1;
    private Properties credProperties2;
    private KafkaCredentials creds1;
    private KafkaCredentials creds2;
    private JobID jobId1 = JobID.generate();
    private JobID jobId2 = JobID.generate();
    private Configuration configuration;
    private ExecutorService executor;

    private KafkaCredentialsCacheImpl credentialsCache;

    @Before
    public void setUp() throws IOException, NoSuchAlgorithmException {
        credentialsCache = new KafkaCredentialsCacheImpl();

        credProperties1 = new Properties();
        credProperties1.setProperty("a", "b");
        credProperties1.setProperty("c", "d");
        creds1 = new KafkaCredentials("dpat1");
        credProperties2 = new Properties();
        credProperties2.setProperty("e", "f");
        credProperties2.setProperty("g", "h");
        creds2 = new KafkaCredentials("dpat2");

        configuration = new Configuration();
        configuration.setLong(KafkaCredentialsCacheOptions.RECEIVE_TIMEOUT_MS, 50);

        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void testReceive_one() throws Exception {
        Map<JobID, KafkaCredentials> map = new HashMap<>();
        map.put(jobId1, creds1);
        credentialsCache.onNewCredentialsObtained(map);
        Optional<KafkaCredentials> result = credentialsCache.getCredentials(jobId1);

        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(creds1);
    }

    @Test
    public void testReceive_two() throws Exception {
        Map<JobID, KafkaCredentials> map = new HashMap<>();
        map.put(jobId1, creds1);
        map.put(jobId2, creds2);
        credentialsCache.onNewCredentialsObtained(map);
        Optional<KafkaCredentials> result = credentialsCache.getCredentials(jobId1);

        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(creds1);

        result = credentialsCache.getCredentials(jobId2);
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(creds2);
    }

    @Test
    public void testReceive_waitUntilDataArrives() throws Exception {
        credentialsCache.init(configuration);
        Map<JobID, KafkaCredentials> map = new HashMap<>();
        map.put(jobId1, creds1);
        map.put(jobId2, creds2);
        executor.submit(
                () -> {
                    try {
                        Thread.sleep(10);
                        credentialsCache.onNewCredentialsObtained(map);
                    } catch (Exception e) {
                        fail("Got exception", e);
                    }
                });
        Optional<KafkaCredentials> result =
                credentialsCache.getCredentials(new ManualClock(), jobId1);

        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(creds1);

        result = credentialsCache.getCredentials(jobId2);
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(creds2);
    }

    @Test
    public void testReceive_interrupted() throws Throwable {
        credentialsCache.init(configuration);
        AtomicReference<Thread> thread = new AtomicReference<>();
        AtomicBoolean isInterrupted = new AtomicBoolean(false);
        AtomicBoolean isDone = new AtomicBoolean(false);
        executor.submit(
                () -> {
                    try {
                        thread.set(Thread.currentThread());
                        credentialsCache.getCredentials(new ManualClock(), jobId1);
                    } catch (Exception e) {
                        isInterrupted.set(Thread.currentThread().isInterrupted());
                        return;
                    } finally {
                        isDone.set(true);
                    }
                    fail("Shouldn't get to here");
                });

        while (thread.get() == null) {
            Thread.sleep(10);
        }
        thread.get().interrupt();
        while (!isDone.get()) {
            Thread.sleep(10);
        }
        assertThat(isInterrupted.get()).isTrue();
    }

    @Test
    public void testReceive_neverArrives() throws Exception {
        credentialsCache.init(configuration);
        assertThatThrownBy(() -> credentialsCache.getCredentials(jobId1))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage("Timed out while waiting for credentials");
    }

    @Test
    public void testReceive_neverArrives_noError() throws Exception {
        configuration.setBoolean(KafkaCredentialsCacheOptions.RECEIVE_ERROR_ON_TIMEOUT, false);
        credentialsCache.init(configuration);
        Optional<KafkaCredentials> result = credentialsCache.getCredentials(jobId1);
        assertThat(result.isPresent()).isFalse();
    }
}
