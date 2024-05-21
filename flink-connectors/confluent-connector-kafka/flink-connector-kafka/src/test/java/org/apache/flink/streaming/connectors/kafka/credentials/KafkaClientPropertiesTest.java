/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;

import org.junit.Test;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.credentials.KafkaClientProperties.DPAT_ENABLED_PROPERTY;
import static org.apache.flink.streaming.connectors.kafka.credentials.KafkaClientProperties.LOGICAL_CLUSTER_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link KafkaClientProperties}. */
@Confluent
public class KafkaClientPropertiesTest {

    @Test
    public void test_addToProperties() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(LOGICAL_CLUSTER_PROPERTY, "lkc-abc");
        properties.setProperty(DPAT_ENABLED_PROPERTY, "true");
        JobID jobID = new JobID(10, 20);
        KafkaClientProperties.addCredentialsToProperties(jobID, properties);
        assertThat(properties).contains(entry("sasl.mechanism", "OAUTHBEARER"));
        assertThat(properties).contains(entry("security.protocol", "SASL_SSL"));
        assertThat(properties)
                .contains(
                        entry(
                                "sasl.login.callback.handler.class",
                                "org.apache.flink.streaming.connectors.kafka.credentials."
                                        + "OAuthBearerLoginCallbackHandler"));
        assertThat(properties)
                .contains(
                        entry(
                                "sasl.jaas.config",
                                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
                                        + "required logicalClusterId='lkc-abc' "
                                        + "jobId='000000000000000a0000000000000014';"));
    }

    @Test
    public void test_noCluster() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(DPAT_ENABLED_PROPERTY, "true");
        JobID jobID = new JobID(10, 20);

        assertThatThrownBy(
                        () -> KafkaClientProperties.addCredentialsToProperties(jobID, properties))
                .hasMessage("Missing existing table property confluent.kafka.logical.cluster.id");
    }

    @Test
    public void test_disabled() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(DPAT_ENABLED_PROPERTY, "false");
        JobID jobID = new JobID(10, 20);

        KafkaClientProperties.addCredentialsToProperties(jobID, properties);
        assertThat(properties.size()).isEqualTo(1);
    }
}
