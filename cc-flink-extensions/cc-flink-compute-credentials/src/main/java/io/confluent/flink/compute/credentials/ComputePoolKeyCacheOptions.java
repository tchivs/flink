/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.compute.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for the {@link ComputePoolKeyCache}. */
@Confluent
public class ComputePoolKeyCacheOptions {

    public static final ConfigOption<Long> RECEIVE_TIMEOUT_MS =
            ConfigOptions.key("confluent.compute.pool.key.receive.timeout.ms")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "The timeout in ms to wait for the compute pool key to arrive");

    public static final ConfigOption<Boolean> RECEIVE_ERROR_ON_TIMEOUT =
            ConfigOptions.key("confluent.compute.pool.key.receive.error_on_timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether it's considered an error to not receive the compute pool key");
}
