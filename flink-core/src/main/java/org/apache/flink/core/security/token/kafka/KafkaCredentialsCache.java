/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token.kafka;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Used to set a retriever for Kafka credentials, and then provide a method for fetching {@link
 * KafkaCredentials} from that retriever. This is largely used for creating a thin shared layer
 * between Kafka and Schema Registry in flink-core so that the connector/format can share the
 * retriever logic.
 */
@Confluent
public class KafkaCredentialsCache {

    private static KafkaCredentialsRetriever cacheRetriever;

    public static synchronized void setCacheRetriever(KafkaCredentialsRetriever cacheRetriever) {
        KafkaCredentialsCache.cacheRetriever = cacheRetriever;
    }

    public static synchronized Optional<KafkaCredentials> getCredentials(JobID jobID) {
        checkNotNull(cacheRetriever, "Didn't set the Cache Retriever");
        return cacheRetriever.retrieve(jobID);
    }

    /** Interface for the retriever actually fetching Kafka Credentials. */
    @Confluent
    public interface KafkaCredentialsRetriever {

        /**
         * Retrieve the Kafka Credentials. This may block for some amount of time if fetching or
         * waiting for the credentials is appropriate.
         *
         * @param jobID The job id of the owning job.
         * @return Credentials, if they exist
         */
        Optional<KafkaCredentials> retrieve(JobID jobID);
    }
}
