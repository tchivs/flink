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

    public static final ConfigOption<String> STANDBY_HOST =
            key("taskmanager.standby-host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The host where the TaskManager standby activation endpoint is exposed.");

    public static final ConfigOption<String> HOST_ENVIRONMENT_VARIABLE =
            key("confluent.taskmanager.host.environment-variable")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The environment variable that specifies the host name or address where the TaskManager is exposed.");

    public static final ConfigOption<String> FCP_RUNTIME_VERSION =
            key("flink.fcp.runtime.version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Flink runtime version as propagated from the control plane.");

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private TaskManagerConfluentOptions() {}
}
