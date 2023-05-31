/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.util.MurmurHashUtil;

import static org.apache.flink.table.data.binary.BinaryRowDataUtil.BYTE_ARRAY_BASE_OFFSET;

/**
 * {@link FlinkKafkaPartitioner} that ensures that inserts and corresponding retract messages end up
 * in the same partition by hashing the entire row (i.e. value).
 *
 * <p>The default partitioner in retract mode must not be round-robin. Additions and their
 * retractions always need to end up in the same partition to cancel each other out. If users have
 * configured an alternative "hash by key" strategy, this is acceptable for retraction messages as
 * every key is a subset of the entire row. However, in this case this partitioner must not be used.
 */
@Confluent
public final class ConfluentManagedRetractPartitioner extends FlinkKafkaPartitioner<RowData> {

    public static final ConfluentManagedRetractPartitioner INSTANCE =
            new ConfluentManagedRetractPartitioner();

    @Override
    public int partition(
            RowData record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        // The key should be empty, this should have been checked before.
        // Thus, the key is not involved in the partitioning.
        final int hash =
                MurmurHashUtil.hashUnsafeBytes(value, BYTE_ARRAY_BASE_OFFSET, value.length);
        return partitions[Math.abs(hash) % partitions.length];
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ConfluentManagedRetractPartitioner;
    }

    @Override
    public int hashCode() {
        return ConfluentManagedRetractPartitioner.class.hashCode();
    }
}
