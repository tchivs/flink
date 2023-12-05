/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.docs.Documentation;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Specific for Confluent configuration options for the JobManager. */
@Confluent
public class JobManagerConfluentOptions {

    /** Options that should be taken from JobManager while it takes over standby TaskManager. */
    @Documentation.ExcludeFromDocumentation("Confluent internal feature")
    public static final ConfigOption<List<String>> STANDBY_TASK_MANAGER_OVERRIDE_OPTIONS =
            key("jobmanager.overridden-standby-task-manager-options")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Semicolon-separate list of options that should be taken from JobManager while it takes over standby TaskManager.");

    private JobManagerConfluentOptions() {
        throw new IllegalAccessError();
    }
}
