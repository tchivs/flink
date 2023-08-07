/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** JSON specific options. */
public class JsonFormatOptions {

    public static final ConfigOption<Boolean> VALIDATE_WRITES =
            ConfigOptions.key("validate-writes")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to validate writes against the schema from Schema Registry before"
                                    + "writing a record to a topic. This ensures all records match the expected JSON schema."
                                    + "Disabling might improve performance, but some JSON schema validations might not be"
                                    + " applied.");

    private JsonFormatOptions() {}
}
