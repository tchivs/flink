/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.contrib.streaming.state.sstmerge;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Lookup helper for RocksDB column families by their name. */
class ColumnFamilyLookup {
    private final Map<Key, ColumnFamilyHandle> map;

    public ColumnFamilyLookup() {
        map = new ConcurrentHashMap<>();
    }

    @Nullable
    public ColumnFamilyHandle get(byte[] name) {
        return map.get(new Key(name));
    }

    public void add(ColumnFamilyHandle handle) {
        try {
            map.put(new Key(handle.getName()), handle);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Key {
        private final byte[] payload;

        private Key(byte[] payload) {
            this.payload = checkNotNull(payload);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Arrays.equals(payload, key.payload);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(payload);
        }
    }
}
