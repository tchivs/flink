/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token.kafka.util;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCache;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Map;
import java.util.Optional;

/** Mocks a {@link KafkaCredentialsCache}. */
@Confluent
public class MockKafkaCredentialsCache implements KafkaCredentialsCache {

    private Map<JobID, KafkaCredentials> credentialsByJobId;
    private boolean error;

    @Override
    public void init(Configuration configuration) {}

    @Override
    public void onNewCredentialsObtained(Map<JobID, KafkaCredentials> credentialsByJobId) {
        this.credentialsByJobId = credentialsByJobId;
    }

    @Override
    public Optional<KafkaCredentials> getCredentials(JobID jobID) {
        if (error) {
            throw new FlinkRuntimeException("Error!");
        }
        return Optional.ofNullable(credentialsByJobId.get(jobID));
    }

    public void withError() {
        error = true;
    }
}
