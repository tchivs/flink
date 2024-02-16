/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.contrib.streaming.state.sstmerge;

import org.rocksdb.ColumnFamilyHandle;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Target for a single manual compaction call. */
class CompactionTask {
    final int level;
    final List<String> files;
    final ColumnFamilyHandle columnFamilyHandle;

    CompactionTask(int level, List<String> files, ColumnFamilyHandle columnFamilyHandle) {
        checkArgument(!files.isEmpty());
        checkArgument(level >= 0);
        this.level = level;
        this.files = checkNotNull(files);
        this.columnFamilyHandle = checkNotNull(columnFamilyHandle);
    }

    @Override
    public String toString() {
        return "CompactionTask{"
                + "level="
                + level
                + ", files="
                + files
                + ", columnFamily="
                + columnFamilyHandle
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompactionTask that = (CompactionTask) o;
        return level == that.level
                && Objects.equals(files, that.files)
                && Objects.equals(columnFamilyHandle, that.columnFamilyHandle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(level, files, columnFamilyHandle);
    }
}
