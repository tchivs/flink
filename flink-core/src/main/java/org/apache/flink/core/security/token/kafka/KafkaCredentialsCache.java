/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token.kafka;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

import java.util.Map;
import java.util.Optional;

/**
 * Used to cache credentials for Kafka and Schema Registry. There must be a periodic service which
 * fetches and them sets the credentials on this cache with {@code onNewCredentialsObtained}. The
 * cache allows calls to {@code getCredentials} enabling callers to wait until they arrive.
 */
@Confluent
public interface KafkaCredentialsCache {

    /**
     * Initializes the cache.
     *
     * @param configuration The Flink Configuration
     */
    void init(Configuration configuration);

    /**
     * Called when new credentials are available.
     *
     * @param credentialsByJobId The full map of credentials exposed by the cache.
     */
    void onNewCredentialsObtained(Map<JobID, KafkaCredentials> credentialsByJobId);

    /**
     * Gets credentials for the given job, possibly blocking until they are available or a timeout
     * occurs.
     *
     * @param jobID The job to get credentials for
     * @return The credentials if they are available
     */
    Optional<KafkaCredentials> getCredentials(JobID jobID);
}
