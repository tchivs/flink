/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCache;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCacheImpl;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This class receives Kafka tokens within a cluster.
 *
 * <p>In more detail, {@link KafkaDelegationTokenProvider} is created on the JM and {@link
 * DelegationTokenProvider#obtainDelegationTokens()} is called periodically, fetching a map of
 * tokens, serializing them, and doing an rpc to each TM. The TMs all instantiate an instance of
 * {@link KafkaDelegationTokenReceiver} on startup, and upon getting the rpc, pass the serialized
 * token to the receiver. This allows this class to do the fetching of tokens in a single place and
 * then fans out the tokens to all the TMs.
 */
@Confluent
public class KafkaDelegationTokenReceiver implements DelegationTokenReceiver {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaDelegationTokenReceiver.class);

    private final KafkaCredentialsCache kafkaCredentialsCache;

    public KafkaDelegationTokenReceiver() {
        this.kafkaCredentialsCache = KafkaCredentialsCacheImpl.INSTANCE;
    }

    @VisibleForTesting
    public KafkaDelegationTokenReceiver(KafkaCredentialsCache kafkaCredentialsCache) {
        this.kafkaCredentialsCache = kafkaCredentialsCache;
    }

    @Override
    public String serviceName() {
        return "Kafka";
    }

    @Override
    public synchronized void init(Configuration configuration) {
        kafkaCredentialsCache.init(configuration);
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        Map<JobID, KafkaCredentials> credentialsByJobId =
                InstantiationUtil.deserializeObject(
                        tokens, KafkaDelegationTokenReceiver.class.getClassLoader());
        LOG.info("Received credentials for jobs {}", credentialsByJobId.keySet());
        kafkaCredentialsCache.onNewCredentialsObtained(credentialsByJobId);
    }
}
