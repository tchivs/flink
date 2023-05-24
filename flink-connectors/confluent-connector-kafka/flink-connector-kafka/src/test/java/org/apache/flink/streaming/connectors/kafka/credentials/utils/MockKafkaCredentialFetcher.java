/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.streaming.connectors.kafka.credentials.JobCredentialsMetadata;
import org.apache.flink.streaming.connectors.kafka.credentials.KafkaCredentialFetcher;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/** Mocks {@link KafkaCredentialFetcher}. */
@Confluent
public class MockKafkaCredentialFetcher implements KafkaCredentialFetcher {

    private List<KafkaCredentials> kafkaCredentials = new ArrayList<>();
    private List<JobCredentialsMetadata> fetchParameters = new ArrayList<>();
    private boolean errorThrown;

    public MockKafkaCredentialFetcher withResponse(KafkaCredentials kafkaCredentials) {
        this.kafkaCredentials.add(kafkaCredentials);
        return this;
    }

    public MockKafkaCredentialFetcher withErrorThrown() {
        this.errorThrown = true;
        return this;
    }

    @Override
    public KafkaCredentials fetchToken(JobCredentialsMetadata jobCredentialsMetadata) {
        fetchParameters.add(jobCredentialsMetadata);
        if (errorThrown) {
            throw new RuntimeException("Error!");
        }
        Preconditions.checkState(kafkaCredentials.size() > 0);
        return kafkaCredentials.remove(0);
    }

    public List<JobCredentialsMetadata> getFetchParametersForAllCalls() {
        return fetchParameters;
    }

    protected void init() {}
}
