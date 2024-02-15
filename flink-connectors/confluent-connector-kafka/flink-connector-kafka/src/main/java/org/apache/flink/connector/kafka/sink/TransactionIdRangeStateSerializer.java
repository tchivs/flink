/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Serializer for {@link TransactionIdRangeState}. */
@Confluent
@Internal
public class TransactionIdRangeStateSerializer
        implements SimpleVersionedSerializer<TransactionIdRangeState> {

    @Override
    public int getVersion() {
        return TransactionIdRangeState.VERSION;
    }

    @Override
    public byte[] serialize(TransactionIdRangeState obj) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(obj.getStartId());
            out.writeInt(obj.getPoolSize());
            out.writeInt(obj.getNumSubtasks());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public TransactionIdRangeState deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            final int startTransactionId = in.readInt();
            final int poolSize = in.readInt();
            final int numSubtasks = in.readInt();
            return TransactionIdRangeState.fromRestoredState(
                    startTransactionId, poolSize, numSubtasks);
        }
    }
}
