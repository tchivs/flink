/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.api.common;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;

import java.util.Optional;

/** Common runtime context used to isolate Confluent changes. */
@Confluent
@Internal
public interface CommonConfluentContext {

    /** Gets the job id of this job. Optional to make it easier to have a default impl */
    @Confluent
    @Internal
    default Optional<JobID> getJobID() {
        return Optional.empty();
    }
}
