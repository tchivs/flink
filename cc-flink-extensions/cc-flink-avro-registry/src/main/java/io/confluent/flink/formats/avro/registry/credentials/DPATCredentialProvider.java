/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCache;

import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;

import java.net.URL;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This implementation of {@link BearerAuthCredentialProvider} provides the credentials for Schema
 * Registry by taking the DPAT token from the credential cache.
 */
@Confluent
public class DPATCredentialProvider implements BearerAuthCredentialProvider {
    public static final String LOGICAL_CLUSTER_PROPERTY =
            "confluent.schema.registry.logical.cluster.id";
    public static final String JOB_ID_PROPERTY = "jobId";

    private JobID jobID;
    private String targetSchemaRegistry;
    private String targetIdentityPoolId;

    @Override
    public String alias() {
        return "OAUTHBEARER_DPAT";
    }

    @Override
    public String getBearerToken(URL url) {
        checkNotNull(jobID, "Configuration must provide jobId before token is " + "provided");
        Optional<KafkaCredentials> credentials = KafkaCredentialsCache.getCredentials(jobID);
        return credentials.map(KafkaCredentials::getDpatToken).orElse(null);
    }

    @Override
    public String getTargetSchemaRegistry() {
        return this.targetSchemaRegistry;
    }

    @Override
    public String getTargetIdentityPoolId() {
        return this.targetIdentityPoolId;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        checkNotNull(
                configs.get(LOGICAL_CLUSTER_PROPERTY),
                "Logical cluster required using property " + LOGICAL_CLUSTER_PROPERTY);
        targetSchemaRegistry = (String) configs.get(LOGICAL_CLUSTER_PROPERTY);

        // During the planning phase, this provider is created and configured
        // though with no runtime fields set.
        if (configs.get(JOB_ID_PROPERTY) != null) {
            jobID = JobID.fromHexString((String) configs.get(JOB_ID_PROPERTY));
        }
    }
}
