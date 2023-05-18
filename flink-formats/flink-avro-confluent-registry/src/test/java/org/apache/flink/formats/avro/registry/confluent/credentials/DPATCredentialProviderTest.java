/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.formats.avro.registry.confluent.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCache;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

import static org.apache.flink.formats.avro.registry.confluent.credentials.DPATCredentialProvider.JOB_ID_PROPERTY;
import static org.apache.flink.formats.avro.registry.confluent.credentials.DPATCredentialProvider.LOGICAL_CLUSTER_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests {@link DPATCredentialProvider}. */
@Confluent
public class DPATCredentialProviderTest {

    private DPATCredentialProvider provider;

    @BeforeEach
    void setUp() {
        provider = new DPATCredentialProvider();
    }

    @Test
    void testEmptyConfig() {
        assertThatThrownBy(() -> provider.configure(ImmutableMap.of()))
                .hasMessageContaining("Logical cluster required");
    }

    @Test
    void testNoJobId() {
        provider.configure(ImmutableMap.of(LOGICAL_CLUSTER_PROPERTY, "lsrc-abc"));
        assertThatThrownBy(() -> provider.getBearerToken(new URL("http://example.com")))
                .hasMessageContaining("Configuration must provide jobId");
    }

    @Test
    void testSuccess() throws MalformedURLException {
        KafkaCredentialsCache.setCacheRetriever(
                jobID -> Optional.of(new KafkaCredentials("token-123")));
        provider.configure(
                ImmutableMap.of(
                        LOGICAL_CLUSTER_PROPERTY,
                        "lsrc-abc",
                        JOB_ID_PROPERTY,
                        "00000000000000000000000000000abc"));
        assertThat(provider.getBearerToken(new URL("http://example.com"))).isEqualTo("token-123");
    }
}
