/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.contrib.streaming.state.sstmerge;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBProperty;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/** Default implementation of {@link RocksDBManualCompactionManager}. */
class RocksDBManualCompactionManagerImpl implements RocksDBManualCompactionManager {
    private static final Logger LOG =
            LoggerFactory.getLogger(RocksDBManualCompactionManagerImpl.class);

    private final ColumnFamilyLookup lookup;
    private final CompactionScheduler scheduler;
    private final CompactionTracker tracker;

    public RocksDBManualCompactionManagerImpl(
            RocksDB db, RocksDBManualCompactionConfig settings, ExecutorService ioExecutor) {
        this.lookup = new ColumnFamilyLookup();
        this.tracker = new CompactionTracker(settings, cf -> getNumAutoCompactions(db, cf));
        this.scheduler =
                new CompactionScheduler(
                        settings,
                        ioExecutor,
                        new CompactionTaskProducer(db, settings, lookup),
                        new Compactor(db, settings.maxOutputFileSize.getBytes()),
                        tracker);
    }

    @Override
    public void start() {
        scheduler.start();
    }

    @Override
    public void register(RocksDbKvStateInfo stateInfo) {
        LOG.debug("Register state for manual compactions: '{}'", stateInfo.metaInfo.getName());
        lookup.add(stateInfo.columnFamilyHandle);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Stopping RocksDBManualCompactionManager");
        tracker.close();
        try {
            scheduler.stop();
        } catch (Exception e) {
            LOG.warn("Unable to stop compaction scheduler {}", scheduler, e);
        }
    }

    private static long getNumAutoCompactions(RocksDB db, ColumnFamilyHandle columnFamily) {
        try {
            return db.getLongProperty(
                    columnFamily, RocksDBProperty.NumRunningCompactions.getRocksDBProperty());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
