/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.contrib.streaming.state.sstmerge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

/**
 * Schedules manual compactions of small disjoint SST files created by RocksDB. It does so
 * periodically while maintaining {@link RocksDBManualCompactionOptions#MIN_INTERVAL} between
 * compaction rounds, where each round is at most {@link
 * RocksDBManualCompactionOptions#MAX_PARALLEL_COMPACTIONS}.
 */
class CompactionScheduler {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionScheduler.class);

    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService ioExecutor;
    private final long checkPeriodMs;
    private final CompactionTracker tracker;
    private final Compactor compactor;
    private final CompactionTaskProducer taskProducer;

    public CompactionScheduler(
            RocksDBManualCompactionConfig settings,
            ExecutorService ioExecutor,
            CompactionTaskProducer taskProducer,
            Compactor compactor,
            CompactionTracker tracker) {
        this.ioExecutor = ioExecutor;
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.checkPeriodMs = settings.minInterval;
        this.tracker = tracker;
        this.compactor = compactor;
        this.taskProducer = taskProducer;
    }

    public void start() {
        scheduleScan();
    }

    public void stop() throws InterruptedException {
        scheduledExecutor.shutdownNow();
    }

    public void scheduleScan() {
        LOG.trace("Schedule SST scan in {} ms", checkPeriodMs);
        scheduledExecutor.schedule(
                () -> ioExecutor.execute(this::maybeScan), checkPeriodMs, TimeUnit.MILLISECONDS);
    }

    public void maybeScan() {
        LOG.trace("Starting SST scan");
        if (tracker.haveManualCompactions() || tracker.isShuttingDown()) {
            LOG.trace("Skip SST scan {}", tracker);
            // nothing to do:
            // previous compactions didn't finish yet
            // the last one will reschedule this task
            return;
        }

        final List<CompactionTask> targets = scan();
        LOG.trace("SST scan resulted in targets {}", targets);
        if (targets.isEmpty()) {
            scheduleScan();
            return;
        }

        for (CompactionTask target : targets) {
            ioExecutor.execute(
                    () ->
                            tracker.runWithTracking(
                                    target.columnFamilyHandle,
                                    () ->
                                            compactor.compact(
                                                    target.columnFamilyHandle,
                                                    target.level,
                                                    target.files),
                                    this::scheduleScan));
        }
    }

    private List<CompactionTask> scan() {
        try {
            return taskProducer.produce();
        } catch (Exception e) {
            LOG.warn("Unable to scan for compaction targets", e);
            return emptyList();
        }
    }
}
