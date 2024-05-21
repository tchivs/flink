/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.contrib.streaming.state.sstmerge;

import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;

import org.junit.jupiter.api.Test;

import java.util.Collections;

class CompactionSchedulerTest {
    @Test
    void testClose() throws InterruptedException {
        RocksDBManualCompactionConfig config = RocksDBManualCompactionConfig.getDefault();
        ManuallyTriggeredScheduledExecutorService scheduledExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        CompactionScheduler compactionScheduler =
                new CompactionScheduler(
                        config,
                        ioExecutor,
                        new CompactionTaskProducer(
                                Collections::emptyList, config, new ColumnFamilyLookup()),
                        new Compactor(Compactor.CompactionTarget.NO_OP, 1L),
                        new CompactionTracker(config, ign -> 0L),
                        scheduledExecutor);
        compactionScheduler.start();
        scheduledExecutor.triggerScheduledTasks();
        compactionScheduler.stop();
        ioExecutor.triggerAll(); // should not fail e.g. because compactionScheduler was stopped
        ioExecutor.shutdown();
    }
}
