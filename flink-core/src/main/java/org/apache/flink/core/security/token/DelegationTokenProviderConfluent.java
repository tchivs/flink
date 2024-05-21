/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;

/** A DelegationTokenProvider with some additional methods. */
@Confluent
public interface DelegationTokenProviderConfluent {
    /**
     * Registers a new job that started with the provider, which may need to do job specific
     * fetching for the token. If so, the method can return true and trigger another refresh.
     *
     * <p>This call must not fail since it could leave things in an inconsistent state.
     *
     * @param jobId The job id of the job
     * @param jobConfiguration The job configuration
     * @return If the newly registered job should trigger an immediate call to {@code
     *     obtainDelegationTokens} or not.
     */
    default boolean registerJob(JobID jobId, Configuration jobConfiguration) throws Exception {
        return false;
    }

    /**
     * Called when the job has ended and should be unregistered.
     *
     * <p>This call must not fail since it could leave things in an inconsistent state.
     *
     * @param jobId The job id of the job
     */
    default void unregisterJob(JobID jobId) throws Exception {}

    /** Stops the provider. Any resources should be closed. */
    default void stop() {}
}
