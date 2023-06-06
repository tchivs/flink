/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.docs.Documentation;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of specific Confluent configuration options relating to TaskManager and Task settings.
 */
@Confluent
public class TaskManagerConfluentOptions {

    /**
     * Allowing to start TaskManager in standby mode - without an immediate connection to HA or
     * other services.
     */
    @Documentation.ExcludeFromDocumentation("Confluent internal feature")
    public static final ConfigOption<Boolean> STANDBY_MODE =
            key("taskmanager.standby-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allowing to start TaskManager in standby mode - without an immediate connection to HA or other services.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private TaskManagerConfluentOptions() {}
}
