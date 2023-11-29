/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;

/** Extension of RestfulGateway with Confluent specific methods. */
@Confluent
public interface ConfluentRestfulGateway {

    default CompletableFuture<Void> failJob(JobID jobId, Exception message, Time timeout) {
        return FutureUtils.completedExceptionally(new UnsupportedOperationException());
    }
}
