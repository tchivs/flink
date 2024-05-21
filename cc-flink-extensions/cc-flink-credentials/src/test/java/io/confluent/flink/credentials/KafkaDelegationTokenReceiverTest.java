/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.util.MockKafkaCredentialsCache;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaDelegationTokenReceiver}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class KafkaDelegationTokenReceiverTest {

    private KafkaCredentials creds1;
    private KafkaCredentials creds2;
    private JobID jobId1 = JobID.generate();
    private JobID jobId2 = JobID.generate();

    private MockKafkaCredentialsCache credentialsCache;
    private KafkaDelegationTokenReceiver receiver;

    @BeforeEach
    public void setUp() {
        credentialsCache = new MockKafkaCredentialsCache();
        receiver = new KafkaDelegationTokenReceiver(credentialsCache);
        creds1 = new KafkaCredentials("dpat1");
        creds2 = new KafkaCredentials("dpat2");
    }

    @Test
    public void testReceive() throws Exception {

        receiver.onNewTokensObtained(
                InstantiationUtil.serializeObject(ImmutableMap.of(jobId1, creds1, jobId2, creds2)));

        assertThat(credentialsCache.getCredentials(jobId1)).contains(creds1);
        assertThat(credentialsCache.getCredentials(jobId2)).contains(creds2);

        receiver.onNewTokensObtained(
                InstantiationUtil.serializeObject(ImmutableMap.of(jobId1, creds1)));

        assertThat(credentialsCache.getCredentials(jobId1)).contains(creds1);
        assertThat(credentialsCache.getCredentials(jobId2)).isEmpty();
    }
}
