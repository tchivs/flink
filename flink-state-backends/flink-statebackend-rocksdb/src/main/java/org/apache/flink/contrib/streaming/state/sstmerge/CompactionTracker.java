/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.contrib.streaming.state.sstmerge;

import org.apache.flink.util.function.RunnableWithException;

import jdk.internal.org.jline.utils.Log;
import org.rocksdb.ColumnFamilyHandle;

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Function;

/**
 * Tracks the number of pending/running compactions (manual and automatic) and the DB status. Used
 * concurrently by different compaction threads and by SST scanning threads.
 */
@ThreadSafe
class CompactionTracker {
    private final Function<ColumnFamilyHandle, Long> runningAutoCompactions;
    private final int maxManualCompactions;
    private final int maxAutoCompactions;
    private int pendingManualCompactions;
    private int runningManualCompactions;
    private boolean isShuttingDown;

    public CompactionTracker(
            RocksDBManualCompactionConfig settings,
            Function<ColumnFamilyHandle, Long> runningAutoCompactions) {
        this.maxManualCompactions = settings.maxManualCompactions;
        this.maxAutoCompactions = settings.maxAutoCompactions;
        this.runningAutoCompactions = runningAutoCompactions;
        this.isShuttingDown = false;
    }

    private synchronized void complete() {
        runningManualCompactions--;
    }

    private synchronized void cancel() {
        pendingManualCompactions--;
    }

    private synchronized boolean tryStart(ColumnFamilyHandle cf) {
        if (runningManualCompactions >= maxManualCompactions) {
            return false;
        }
        if (isShuttingDown()) {
            return false;
        }
        if (runningAutoCompactions.apply(cf) >= maxAutoCompactions) {
            return false;
        }
        // all good
        pendingManualCompactions--;
        runningManualCompactions++;
        return true;
    }

    private synchronized void runIfNoManualCompactions(Runnable runnable) {
        if (!haveManualCompactions()) {
            runnable.run();
        }
    }

    public synchronized boolean haveManualCompactions() {
        return runningManualCompactions > 0 || pendingManualCompactions > 0;
    }

    public synchronized boolean isShuttingDown() {
        return isShuttingDown;
    }

    public void close() {
        isShuttingDown = true;
    }

    @Override
    public String toString() {
        return "CompactionTracker{"
                + "maxManualCompactions="
                + maxManualCompactions
                + ", maxAutoCompactions="
                + maxAutoCompactions
                + ", pendingManualCompactions="
                + pendingManualCompactions
                + ", runningManualCompactions="
                + runningManualCompactions
                + ", isShuttingDown="
                + isShuttingDown
                + '}';
    }

    void runWithTracking(
            ColumnFamilyHandle columnFamily,
            RunnableWithException compaction,
            Runnable lastCompactionPostAction) {
        if (tryStart(columnFamily)) {
            try {
                compaction.run();
            } catch (Exception e) {
                Log.warn("Unable to compact {} (concurrent compaction?)", compaction, e);
            }
            complete();
        } else {
            // drop this task - new will be created with a fresh set of files
            cancel();
        }
        // we were the last manual compaction - schedule the scan
        runIfNoManualCompactions(lastCompactionPostAction);
    }
}
