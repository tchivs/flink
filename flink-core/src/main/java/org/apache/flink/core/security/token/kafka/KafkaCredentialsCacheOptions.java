/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token.kafka;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for the {@link KafkaCredentialsCache}. */
@Confluent
public class KafkaCredentialsCacheOptions {

    public static final ConfigOption<Long> RECEIVE_TIMEOUT_MS =
            ConfigOptions.key("confluent.kafka.credential.receive.timeout.ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription("The timeout in ms to wait for credentials to arrive");

    public static final ConfigOption<Boolean> RECEIVE_ERROR_ON_TIMEOUT =
            ConfigOptions.key("confluent.kafka.credential.receive.error_on_timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether it's considered an error to not receive credentials");
}
