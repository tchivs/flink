/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.api;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

/** Options to be used by the adapter. */
public class AdapterOptions {
    public static final String ADAPTER_PREFIX = "confluent.remote-udf.adapter.";

    public static final ConfigOption<Integer> ADAPTER_PARALLELISM =
            ConfigOptions.key(ADAPTER_PREFIX + "parallelism")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The parallelism to allow for.");

    public static final ConfigOption<Duration> ADAPTER_HANDLER_WAIT_TIMEOUT =
            ConfigOptions.key(ADAPTER_PREFIX + "handler.wait.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("How long to wait for a slot to be handled before giving up.");
}
