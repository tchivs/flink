/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.util.MockKafkaCredentialsCache;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link io.confluent.flink.formats.registry.credentials.DPATCredentialProvider}. */
@Confluent
class DPATCredentialProviderTest {
    private static final JobID JOB_ID = JobID.fromHexString("00000000000000000000000000000abc");

    private DPATCredentialProvider provider;
    private MockKafkaCredentialsCache kafkaCredentialsCache;

    @BeforeEach
    void setUp() {
        kafkaCredentialsCache = new MockKafkaCredentialsCache();
        provider = new DPATCredentialProvider(kafkaCredentialsCache);
    }

    @Test
    void testEmptyConfig() {
        assertThatThrownBy(() -> provider.configure(ImmutableMap.of()))
                .hasMessageContaining("Logical cluster required");
    }

    @Test
    void testNoJobId() {
        provider.configure(
                ImmutableMap.of(DPATCredentialProvider.LOGICAL_CLUSTER_PROPERTY, "lsrc-abc"));
        assertThatThrownBy(() -> provider.getBearerToken(new URL("http://example.com")))
                .hasMessageContaining("Configuration must provide jobId");
    }

    @Test
    void testSuccess() throws MalformedURLException {
        kafkaCredentialsCache.onNewCredentialsObtained(
                ImmutableMap.of(JOB_ID, new KafkaCredentials("token-123")));
        provider.configure(
                ImmutableMap.of(
                        DPATCredentialProvider.LOGICAL_CLUSTER_PROPERTY,
                        "lsrc-abc",
                        DPATCredentialProvider.JOB_ID_PROPERTY,
                        JOB_ID.toHexString()));
        assertThat(provider.getBearerToken(new URL("http://example.com"))).isEqualTo("token-123");
    }
}
