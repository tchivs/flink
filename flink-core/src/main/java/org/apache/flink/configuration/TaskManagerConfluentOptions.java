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
@Documentation.ExcludeFromDocumentation("Confluent internal feature")
public class TaskManagerConfluentOptions {

    /**
     * Allowing to start TaskManager in standby mode - without an immediate connection to HA or
     * other services.
     */
    public static final ConfigOption<Boolean> STANDBY_MODE =
            key("taskmanager.standby-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allowing to start TaskManager in standby mode - without an immediate connection to HA or other services.");

    public static final ConfigOption<Integer> STANDBY_RPC_PORT =
            key("taskmanager.rpc.standby-port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The external RPC port where the TaskManager standby activation endpoint is exposed.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private TaskManagerConfluentOptions() {}
}
