/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.contrib.streaming.state.sstmerge;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Compacts multiple RocksDB SST files using {@link RocksDB#compactFiles(CompactionOptions,
 * ColumnFamilyHandle, List, int, int, CompactionJobInfo) RocksDB#compactFiles} into the last level.
 * Usually this results in a single SST file if it doesn't exceed RocksDB target output file size
 * for that level.
 */
class Compactor {
    private static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
    private static final int OUTPUT_PATH_ID = 0; // just use the first one

    private final RocksDB db;
    private final long targetOutputFileSize;

    public Compactor(RocksDB db, long targetOutputFileSize) {
        this.db = db;
        this.targetOutputFileSize = targetOutputFileSize;
    }

    void compact(ColumnFamilyHandle cfName, int level, List<String> files) throws RocksDBException {
        int outputLevel = Math.min(level + 1, cfName.getDescriptor().getOptions().numLevels() - 1);
        LOG.debug(
                "Manually compacting {} files from level {} to {}: {}",
                files.size(),
                level,
                outputLevel,
                files);
        try (CompactionOptions options =
                        new CompactionOptions().setOutputFileSizeLimit(targetOutputFileSize);
                CompactionJobInfo compactionJobInfo = new CompactionJobInfo()) {
            db.compactFiles(options, cfName, files, outputLevel, OUTPUT_PATH_ID, compactionJobInfo);
        }
    }
}
