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
@Documentation.ExcludeFromDocumentation("Confluent internal feature")
public class JobManagerConfluentOptions {

    /** Options that should be taken from JobManager while it takes over standby TaskManager. */
    public static final ConfigOption<List<String>> STANDBY_TASK_MANAGER_OVERRIDE_OPTIONS =
            key("jobmanager.overridden-standby-task-manager-options")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Semicolon-separate list of options that should be taken from JobManager while it takes over standby TaskManager.");

    /**
     * Controls whether the job HA checkpoint store data (everything written to HA from a
     * CompletedCheckpointStore)) is retained when a job terminates.
     */
    public static final ConfigOption<Boolean> RETAIN_JOB_HA_CP_STORE_ON_TERMINATION =
            key("confluent.high-availability.retain-job-ha-cp-store-on-termination")
                    .booleanType()
                    .defaultValue(false);

    private JobManagerConfluentOptions() {
        throw new IllegalAccessError();
    }
}
