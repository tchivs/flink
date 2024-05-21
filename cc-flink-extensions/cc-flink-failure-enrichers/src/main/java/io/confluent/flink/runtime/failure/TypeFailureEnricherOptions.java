/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Config options for {@link TypeFailureEnricher}. */
public final class TypeFailureEnricherOptions {

    private TypeFailureEnricherOptions() {}

    public static final ConfigOption<Boolean> ENABLE_JOB_CANNOT_RESTART_LABEL =
            ConfigOptions.key("confluent.failure-enrichers.type.enable_job_cannot_restart_label")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Enables TypeFailureEnricher to tag errors with FailureEnricher.KEY_JOB_CANNOT_RESTART and suppress their restart.");
}
