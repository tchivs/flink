/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v3;

import io.confluent.flink.jobgraph.JobGraphWrapper;

import java.util.List;
import java.util.Map;

/** Generate the JobGraph from the given arguments. */
public interface JobGraphGeneratorV3 {

    /** Prefix to mark options that have been introduced by Confluent. */
    String CONFLUENT_PREFIX = "confluent.";

    /** Prefix to mark options that have been introduced by Confluent and set by the user. */
    String CONFLUENT_USER_PREFIX = CONFLUENT_PREFIX + "user.";

    /**
     * Preload resources required for the job graph generation. This method _may_ be called, exactly
     * once, some time before {@link #generateJobGraph}.
     *
     * <p>The primary purpose is to eagerly load classes required for the job graph generation.
     */
    void prepareGeneration();

    /**
     * Generate the JobGraph from the given arguments. For Streaming SQL, the arguments would
     * contain a CompiledPlan.
     *
     * <p>After the graph is generated by this method, it will be adjusted by setting savepoint path
     * and initial job requirements and submitted for execution.
     *
     * <p>This method may be called multiple times on the same generator instance.
     *
     * @param arguments the arguments passed from SQL service via FCP.
     * @param allOptions includes Flink cluster configuration, Flink job configuration, and
     *     Confluent-specific job options. It assumes that Confluent-specific options are prefixed
     *     with {@link #CONFLUENT_PREFIX} or {@link #CONFLUENT_USER_PREFIX} to avoid any naming
     *     conflicts with existing or future Flink options. {@link #CONFLUENT_USER_PREFIX} is
     *     removed during generation after it was checked and validated that it's safe to do so.
     */
    JobGraphWrapper generateJobGraph(List<String> arguments, Map<String, String> allOptions);
}
