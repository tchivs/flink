/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.ArrayList;
import java.util.List;

/** Mocks something equivalent to Thread.sleep. */
@Confluent
public class MockSleeper implements ThrowingConsumer<Long, InterruptedException> {

    private List<Long> sleeps = new ArrayList<>();

    private boolean interrupt;

    public void withInterrupt() {
        interrupt = true;
    }

    @Override
    public void accept(Long sleepMs) throws InterruptedException {
        if (interrupt) {
            throw new InterruptedException("Interrupt!");
        }
        sleeps.add(sleepMs);
    }

    public List<Long> getSleeps() {
        return sleeps;
    }
}
