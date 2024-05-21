/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;

/** Extension of JobManagerRunner with Confluent specific methods. */
@Confluent
public interface ConfluentJobManagerRunner {

    default CompletableFuture<Void> fail(Exception message, Time timeout) {
        return FutureUtils.completedExceptionally(new UnsupportedOperationException());
    }
}
