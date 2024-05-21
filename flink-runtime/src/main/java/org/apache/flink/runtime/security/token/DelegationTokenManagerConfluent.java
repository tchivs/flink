/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.security.token;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

/** Adds additional methods to DelegationTokenManager. */
@Confluent
public interface DelegationTokenManagerConfluent {
    /**
     * Called when a new job is registered.
     *
     * @param jobId The job id which just started
     * @param jobConfiguration The job's configuration
     */
    default void registerJob(JobID jobId, Configuration jobConfiguration) throws Exception {}

    /**
     * Called after the job is stopped or completes. Must be idempotent.
     *
     * @param jobId The job id of the job
     */
    default void unregisterJob(JobID jobId) throws Exception {}
}
