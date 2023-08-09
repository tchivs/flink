/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.configuration;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Confluent specific {@link ConfigOption}s for a ServiceTask. */
@Confluent
public class ServiceTaskOptions {

    public static final String PRIVATE_PREFIX = "confluent.";
    public static final ConfigOption<Boolean> USE_CONFLUENT_AI_FUNCTIONS =
            ConfigOptions.key(PRIVATE_PREFIX + "ai-functions.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("A flag to enable or disable Confluent AI functions.");
}
