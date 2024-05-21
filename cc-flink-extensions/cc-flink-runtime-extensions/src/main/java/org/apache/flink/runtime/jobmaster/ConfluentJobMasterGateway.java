/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.concurrent.CompletableFuture;

/** Extension of JobMasterGateway with Confluent specific methods. */
@Confluent
public interface ConfluentJobMasterGateway {

    default CompletableFuture<Void> fail(Exception error, Time timeout) {
        return FutureUtils.completedExceptionally(new UnsupportedOperationException());
    }
}
