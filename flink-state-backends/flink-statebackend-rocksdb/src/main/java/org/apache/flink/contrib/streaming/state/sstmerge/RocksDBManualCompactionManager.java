/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.contrib.streaming.state.sstmerge;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * Manages compactions of small and disjoint RocksDB SST files that otherwise would not be merged to
 * reduce write amplification.
 *
 * <p>Such files are usually small and are inlined into the Checkpoint metadata. Which might lead to
 * exceeding RPC message size on checkpoint ACK or recovery.
 *
 * <p>This class manages compactions of one or more Column Families of a single RocksDB instance.
 *
 * <p>Note that "manual" means that the compactions are <b>requested</b> manually (by Flink), but
 * they are still executed by RocksDB.
 */
public interface RocksDBManualCompactionManager extends AutoCloseable {
    Logger LOG = LoggerFactory.getLogger(RocksDBManualCompactionManager.class);

    static RocksDBManualCompactionManager create(
            RocksDB db, RocksDBManualCompactionConfig settings, ExecutorService ioExecutor) {
        LOG.info("Creating RocksDBManualCompactionManager with settings: {}", settings);
        return settings.minInterval <= 0
                ? NO_OP
                : new RocksDBManualCompactionManagerImpl(db, settings, ioExecutor);
    }

    RocksDBManualCompactionManager NO_OP =
            new RocksDBManualCompactionManager() {
                @Override
                public void register(RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo) {}

                @Override
                public void close() {}

                @Override
                public void start() {}
            };

    void register(RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo);

    @Override
    void close() throws Exception;

    void start();
}
