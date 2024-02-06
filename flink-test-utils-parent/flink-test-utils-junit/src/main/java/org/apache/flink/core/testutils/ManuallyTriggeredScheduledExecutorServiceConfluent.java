/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.testutils;

import org.apache.flink.annotation.Confluent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/** Adds some test methods. */
@Confluent
public class ManuallyTriggeredScheduledExecutorServiceConfluent
        extends ManuallyTriggeredScheduledExecutorService {

    @Override
    public Future<?> submit(Runnable task) {
        CompletableFuture<?> future = new CompletableFuture<>();
        execute(
                () -> {
                    try {
                        task.run();
                        future.complete(null);
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
        return future;
    }
}
