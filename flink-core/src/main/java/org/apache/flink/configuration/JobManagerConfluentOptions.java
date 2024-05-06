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

    /** Controls whether savepoints are written to the job HA checkpoint store. */
    public static final ConfigOption<Boolean> STORE_SAVEPOINTS_IN_CHECKPOINT_STORE =
            key("confluent.high-availability.store-savepoints-in-checkpoint-store")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<String> FORCED_WEB_TMP_UI_DIR =
            key("confluent.web.ui.tmpdir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Static base directory from which WebUI files are served. This is similar to 'web.tmpdir', but avoids safety precautions in the ClusterEntrypoint where a random directory is injected.");

    public static final ConfigOption<Boolean> ENABLE_RESOURCE_WAIT_TIMEOUT =
            key("jobmanager.adaptive-scheduler.enable-resource-wait-timeout")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Controls whether the resource-wait-timeout is applied or not.");

    public static final ConfigOption<Boolean> ENABLE_SUBMISSION_ENDPOINT =
            key("confluent.rest.submission.enabled")
                    .booleanType()
                    // disabled by default to avoid noise in the vanilla Flink logs
                    .defaultValue(false)
                    .withDescription(
                            "Controls whether we the proprietary job submission endpoint is enabled.");

    public static final ConfigOption<Integer> REST_MAX_PARALLEL_JOB_SUBMISSIONS =
            key("rest.submissions.max-parallel")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Controls how many job submissions may be processed in parallel.");

    public static final ConfigOption<String> FCP_RUNTIME_VERSION =
            key("flink.fcp.runtime.version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Flink runtime version as propagated from the control plane.");

    private JobManagerConfluentOptions() {
        throw new IllegalAccessError();
    }
}
