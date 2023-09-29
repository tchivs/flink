/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider.ObtainedDelegationTokens;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.clock.ManualClock;

import io.confluent.flink.credentials.utils.MockCredentialDecrypter;
import io.confluent.flink.credentials.utils.MockKafkaCredentialFetcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.confluent.flink.credentials.JobOptions.COMPUTE_POOL_ID;
import static io.confluent.flink.credentials.JobOptions.IDENTITY_POOL_ID;
import static io.confluent.flink.credentials.JobOptions.STATEMENT_ID_CRN;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaDelegationTokenProvider}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class KafkaDelegationTokenProviderTest {

    private MockKafkaCredentialFetcher kafkaDPATCredentialFetcher;
    private MockCredentialDecrypter credentialDecrypter;

    private KafkaDelegationTokenProvider provider;
    private Properties credProperties1;
    private Properties credProperties2;
    private KafkaCredentials creds1;
    private KafkaCredentials creds2;
    private JobID jobId1 = JobID.generate();
    private JobID jobId2 = JobID.generate();
    private Configuration configuration1;
    private Configuration configuration2;
    private ManualClock clock = new ManualClock();
    private ExecutorService executorService;

    @BeforeEach
    public void setUp() {
        kafkaDPATCredentialFetcher = new MockKafkaCredentialFetcher();
        credentialDecrypter = new MockCredentialDecrypter();
        provider =
                new KafkaDelegationTokenProvider(
                        kafkaDPATCredentialFetcher, credentialDecrypter, 100, 5, clock, true);

        credProperties1 = new Properties();
        credProperties1.setProperty("a", "b");
        credProperties1.setProperty("c", "d");
        creds1 = new KafkaCredentials("dpat1");
        credProperties2 = new Properties();
        credProperties2.setProperty("e", "f");
        credProperties2.setProperty("g", "h");
        creds2 = new KafkaCredentials("dpat2");

        configuration1 = new Configuration();
        configuration1.setString(STATEMENT_ID_CRN, "statementId1");
        configuration1.setString(COMPUTE_POOL_ID, "computePoolId1");
        configuration1.setString(IDENTITY_POOL_ID, "identityPoolId1");
        configuration2 = new Configuration();
        configuration2.setString(STATEMENT_ID_CRN, "statementId2");
        configuration2.setString(COMPUTE_POOL_ID, "computePoolId2");
        configuration2.setString(IDENTITY_POOL_ID, "identityPoolId2");
    }

    @AfterEach
    public void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    private Map<JobID, KafkaCredentials> obtainCredentials() throws Exception {
        assertThat(provider.delegationTokensRequired()).isEqualTo(true);
        ObtainedDelegationTokens obtainDelegationTokens = provider.obtainDelegationTokens();
        byte[] tokens = obtainDelegationTokens.getTokens();
        return InstantiationUtil.deserializeObject(tokens, this.getClass().getClassLoader());
    }

    @Test
    public void testFetch_empty() throws Exception {
        kafkaDPATCredentialFetcher.withResponse(creds1);
        Map<JobID, KafkaCredentials> credentials = obtainCredentials();
        assertThat(credentials.size()).isEqualTo(0);
    }

    @Test
    public void testFetch_twoJobs() throws Exception {
        kafkaDPATCredentialFetcher.withResponse(creds1).withResponse(creds2);
        assertThat(provider.registerJob(jobId1, configuration1)).isTrue();
        Map<JobID, KafkaCredentials> credentials = obtainCredentials();
        assertThat(credentials.size()).isEqualTo(1);
        assertThat(credentials).containsKey(jobId1);

        clock.advanceTime(Duration.ofMillis(50));
        assertThat(provider.registerJob(jobId2, configuration2)).isTrue();
        credentials = obtainCredentials();
        assertThat(credentials.size()).isEqualTo(2);
        assertThat(credentials).containsKey(jobId1);
        assertThat(credentials).containsKey(jobId2);
        assertThat(credentials.get(jobId1)).isEqualTo(creds1);
        assertThat(credentials.get(jobId2)).isEqualTo(creds2);

        clock.advanceTime(Duration.ofMillis(100));
        credentials = obtainCredentials();
        assertThat(credentials.size()).isEqualTo(2);

        provider.unregisterJob(jobId1);
        credentials = obtainCredentials();
        assertThat(credentials.size()).isEqualTo(1);
        assertThat(credentials).containsKey(jobId2);

        provider.unregisterJob(jobId2);
        credentials = obtainCredentials();
        assertThat(credentials.size()).isEqualTo(0);

        List<JobCredentialsMetadata> calls =
                kafkaDPATCredentialFetcher.getFetchParametersForAllCalls();
        assertThat(calls.size()).isEqualTo(3);
        assertThat(calls.get(0).getJobID()).isEqualTo(jobId1);
        assertThat(calls.get(1).getJobID()).isEqualTo(jobId2);
        assertThat(calls.get(2).getJobID()).isEqualTo(jobId1);
    }

    @Test
    public void testDisable() throws Exception {
        provider =
                new KafkaDelegationTokenProvider(
                        kafkaDPATCredentialFetcher, credentialDecrypter, 100, 5, clock, false);
        assertThat(provider.delegationTokensRequired()).isEqualTo(false);
    }

    @Test
    public void testFetch_unregisterBeforeDone() throws Exception {
        executorService = Executors.newFixedThreadPool(1);
        AtomicBoolean doneFetching = new AtomicBoolean(false);
        AtomicBoolean isFetching = new AtomicBoolean(false);
        AtomicReference<Map<JobID, KafkaCredentials>> result = new AtomicReference<>();
        kafkaDPATCredentialFetcher.withResponse(
                jcm -> {
                    while (!doneFetching.get()) {
                        try {
                            isFetching.set(true);
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    }
                    return creds1;
                });
        assertThat(provider.registerJob(jobId1, configuration1)).isTrue();
        executorService.submit(
                () -> {
                    try {
                        Map<JobID, KafkaCredentials> credentials = obtainCredentials();
                        result.set(credentials);
                    } catch (Exception e) {
                    }
                });

        while (!isFetching.get()) {
            Thread.sleep(10);
        }
        provider.unregisterJob(jobId1);
        doneFetching.set(true);

        while (result.get() == null) {
            Thread.sleep(10);
        }
        Map<JobID, KafkaCredentials> credentials = result.get();
        assertThat(credentials.size()).isEqualTo(0);
    }

    @Test
    public void testFetch_registerNewBeforeDone() throws Exception {
        executorService = Executors.newFixedThreadPool(1);
        AtomicBoolean doneFetching = new AtomicBoolean(false);
        AtomicBoolean isFetching = new AtomicBoolean(false);
        AtomicReference<Map<JobID, KafkaCredentials>> result = new AtomicReference<>();
        ConcurrentHashMap<JobID, KafkaCredentials> credsMap = new ConcurrentHashMap<>();
        credsMap.put(jobId1, creds1);
        credsMap.put(jobId2, creds2);
        kafkaDPATCredentialFetcher.withResponse(
                jcm -> {
                    while (!doneFetching.get()) {
                        try {
                            isFetching.set(true);
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                        }
                    }
                    return credsMap.get(jcm.getJobID());
                });
        assertThat(provider.registerJob(jobId1, configuration1)).isTrue();
        executorService.submit(
                () -> {
                    try {
                        Map<JobID, KafkaCredentials> credentials = obtainCredentials();
                        result.set(credentials);
                    } catch (Exception e) {
                    }
                });

        while (!isFetching.get()) {
            Thread.sleep(10);
        }
        assertThat(provider.registerJob(jobId2, configuration2)).isTrue();
        doneFetching.set(true);

        while (result.get() == null) {
            Thread.sleep(10);
        }
        // We expect to get just the first job the first time.
        Map<JobID, KafkaCredentials> credentials = result.get();
        assertThat(credentials.size()).isEqualTo(1);
        assertThat(credentials).containsKey(jobId1);

        // We get the second next time we call obtainCredentials.
        credentials = obtainCredentials();
        assertThat(credentials.size()).isEqualTo(2);
        assertThat(credentials).containsKey(jobId1);
        assertThat(credentials).containsKey(jobId2);
    }
}
