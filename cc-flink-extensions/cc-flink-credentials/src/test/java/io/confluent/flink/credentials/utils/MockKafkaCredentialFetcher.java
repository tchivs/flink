/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.credentials.JobCredentialsMetadata;
import io.confluent.flink.credentials.KafkaCredentialFetcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Mocks {@link KafkaCredentialFetcher}. */
@Confluent
public class MockKafkaCredentialFetcher implements KafkaCredentialFetcher {

    private List<KafkaCredentials> kafkaCredentials = new ArrayList<>();
    private List<JobCredentialsMetadata> fetchParameters = new ArrayList<>();
    private Function<JobCredentialsMetadata, KafkaCredentials> responseCallback;
    private boolean errorThrown;

    public MockKafkaCredentialFetcher withResponse(KafkaCredentials kafkaCredentials) {
        this.kafkaCredentials.add(kafkaCredentials);
        this.errorThrown = false;
        return this;
    }

    public MockKafkaCredentialFetcher withResponse(
            Function<JobCredentialsMetadata, KafkaCredentials> responseCallback) {
        this.responseCallback = responseCallback;
        this.errorThrown = false;
        return this;
    }

    public MockKafkaCredentialFetcher withErrorThrown() {
        this.errorThrown = true;
        return this;
    }

    @Override
    public KafkaCredentials fetchToken(JobCredentialsMetadata jobCredentialsMetadata) {
        fetchParameters.add(jobCredentialsMetadata);
        if (responseCallback != null) {
            return responseCallback.apply(jobCredentialsMetadata);
        }
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
