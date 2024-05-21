/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.core.io.SimpleVersionedSerialization.readVersionAndDeSerialize;
import static org.apache.flink.core.io.SimpleVersionedSerialization.writeVersionAndSerialize;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TransactionIdRangeStateSerializer}. */
public class TransactionIdRangeStateSerializerTest {
    private static final TransactionIdRangeStateSerializer SERIALIZER =
            new TransactionIdRangeStateSerializer();

    @Test
    public void testLeftRangeSerde() throws IOException {
        final TransactionIdRangeState leftRange = TransactionIdRangeState.ofLeftRange(3);
        final byte[] serialized = writeVersionAndSerialize(SERIALIZER, leftRange);
        assertThat(readVersionAndDeSerialize(SERIALIZER, serialized)).isEqualTo(leftRange);
    }

    @Test
    public void testRightRangeSerde() throws IOException {
        final TransactionIdRangeState rightRange = TransactionIdRangeState.ofRightRange(3);
        final byte[] serialized = writeVersionAndSerialize(SERIALIZER, rightRange);
        assertThat(readVersionAndDeSerialize(SERIALIZER, serialized)).isEqualTo(rightRange);
    }
}
