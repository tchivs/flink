/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.docs.Documentation;

import java.util.Collections;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Custom metric options. */
@Confluent
public class ConfluentMetricOptions {
    @Documentation.ExcludeFromDocumentation("internal option")
    public static final ConfigOption<Map<String, String>> CUSTOM_METRIC_VARIABLES =
            key("metrics.scope.job.variables.additional")
                    .mapType()
                    .defaultValue(Collections.emptyMap());
}
