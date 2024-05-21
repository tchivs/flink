/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;

/** The interface exposes some runtime info for creating a {@link Committer}. */
@Confluent
@Internal
public interface CommitterContext {

    /** Gets the job id of this job. */
    JobID getJobID();

    int getSubtaskId();
}
