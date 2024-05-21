/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;

/** A class that can fetch KafkaCredentials to be used when accessing CCloud. */
@Confluent
public interface KafkaCredentialFetcher {

    /**
     * Fetches credentials.
     *
     * @param jobCredentialsMetadata The job credential metadata
     * @return The kafka credentials
     */
    KafkaCredentials fetchToken(JobCredentialsMetadata jobCredentialsMetadata);
}
