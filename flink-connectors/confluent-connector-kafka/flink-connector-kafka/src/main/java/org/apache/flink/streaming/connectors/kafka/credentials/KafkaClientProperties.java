/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.Optional;
import java.util.Properties;

/** Utility class to update Kafka client properties to utilize the appropriate credentials. */
@Confluent
public class KafkaClientProperties {
    // A property that we look for
    static final String LOGICAL_CLUSTER_PROPERTY = "confluent.kafka.logical.cluster.id";
    static final String DPAT_ENABLED_PROPERTY = "confluent.kafka.dpat.enabled";

    public static void addCredentialsToProperties(Optional<JobID> jobID, Properties properties) {
        if (!jobID.isPresent()) {
            throw new FlinkRuntimeException("Job id must be present to add credentials");
        }
        addCredentialsToProperties(jobID.get(), properties);
    }

    public static void addCredentialsToProperties(JobID jobID, Properties properties) {
        if (!"true".equals(properties.getProperty(DPAT_ENABLED_PROPERTY))) {
            return;
        }
        String logicalClusterId = properties.getProperty(LOGICAL_CLUSTER_PROPERTY);
        Preconditions.checkNotNull(
                logicalClusterId, "Missing existing table property " + LOGICAL_CLUSTER_PROPERTY);

        properties.setProperty("sasl.mechanism", "OAUTHBEARER");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty(
                "sasl.login.callback.handler.class",
                "org.apache.flink.streaming.connectors.kafka.credentials."
                        + "OAuthBearerLoginCallbackHandler");
        properties.setProperty(
                "sasl.jaas.config",
                String.format(
                        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
                                + "required logicalClusterId='%s' jobId='%s';",
                        logicalClusterId, jobID.toHexString()));
    }
}
