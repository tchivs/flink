/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/** Retry mechanism for calls which may produce exceptions. */
@Confluent
public class CallWithRetry {
    private static final Logger LOG = LoggerFactory.getLogger(CallWithRetry.class);

    public static final double BACKOFF_FACTOR = 1.5;
    private final int maxAttempts;
    private final long delayMs;
    private final ThrowingConsumer<Long, InterruptedException> sleeper;

    public CallWithRetry(int maxAttempts, long delayMs) {
        this(maxAttempts, delayMs, Thread::sleep);
    }

    @VisibleForTesting
    CallWithRetry(
            int maxAttempts, long delayMs, ThrowingConsumer<Long, InterruptedException> sleeper) {
        this.maxAttempts = maxAttempts;
        this.delayMs = delayMs;
        this.sleeper = sleeper;
    }

    public <T> T call(Supplier<T> supplier) {
        long currentDelayMs = delayMs;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                return supplier.get();
            } catch (Throwable t) {
                if (i == maxAttempts - 1) {
                    throw t;
                }
                LOG.error("Got error, retrying on attempt " + i, t);
                try {
                    sleeper.accept(currentDelayMs);
                } catch (InterruptedException e) {
                    throw new FlinkRuntimeException("Interrupted while retrying", e);
                }
                currentDelayMs = (long) (currentDelayMs * BACKOFF_FACTOR);
            }
        }
        throw new IllegalStateException("Should have thrown previous exception");
    }
}
