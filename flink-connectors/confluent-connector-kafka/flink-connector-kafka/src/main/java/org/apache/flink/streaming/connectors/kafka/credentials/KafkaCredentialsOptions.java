/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Options for configuring the fetching and accessing the Confluent DPAT token. */
@Confluent
public class KafkaCredentialsOptions {

    public static final ConfigOption<String> MOUNTED_SECRET =
            ConfigOptions.key("confluent.credential.service.mounted.secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Where to read the fcp secret from disk");

    public static final ConfigOption<String> CREDENTIAL_SERVICE_HOST =
            ConfigOptions.key("confluent.credential.service.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The host of the Flink credential service");

    public static final ConfigOption<Integer> CREDENTIAL_SERVICE_PORT =
            ConfigOptions.key("confluent.credential.service.port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The port of the Flink credential service");

    public static final ConfigOption<String> AUTH_SERVICE_SERVER =
            ConfigOptions.key("confluent.cc.gateway.service.server")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The auth service server, e.g. http://host:port");

    public static final ConfigOption<Boolean> DPAT_ENABLED =
            ConfigOptions.key("confluent.kafka.credential.dpat.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether DPAT fetching and retrieving is enabled to begin with");

    public static final ConfigOption<Long> CREDENTIAL_EXPIRATION_MS =
            ConfigOptions.key("confluent.kafka.credential.expiration.ms")
                    .longType()
                    .defaultValue(Duration.ofMinutes(10).toMillis())
                    .withDescription("How long a DPAT token lasts");

    public static final ConfigOption<Long> CREDENTIAL_CHECK_PERIOD_MS =
            ConfigOptions.key("confluent.kafka.credential.check.period.ms")
                    .longType()
                    .defaultValue(Duration.ofMinutes(2).toMillis())
                    .withDescription(
                            "How often to check for expired kafka token. Should be more "
                                    + "often than confluent.kafka.credential.expiration.ms");
}
