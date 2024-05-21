/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

class KafkaCommittableSerializer implements SimpleVersionedSerializer<KafkaCommittable> {

    @Override
    public int getVersion() {
        return ConfluentKafkaCommittableV1.VERSION;
    }

    @Override
    public byte[] serialize(KafkaCommittable state) throws IOException {
        final ConfluentKafkaCommittableV1 committableV2 =
                ConfluentKafkaCommittableV1.tryCast(state);

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(committableV2.getTransactionalId());
            out.writeUTF(committableV2.getTransactionalIdPrefix());
            out.writeInt(committableV2.getSubtaskId());
            out.writeInt(committableV2.getTransactionIdInPool());
            out.writeInt(committableV2.getIdPoolRangeStartId());
            out.writeInt(committableV2.getIdPoolRangeEndIdExclusive());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public KafkaCommittable deserialize(int version, byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {
            switch (version) {
                case ConfluentKafkaCommittableV1.VERSION:
                    final String transactionIdString = in.readUTF();
                    final String transactionIdPrefix = in.readUTF();
                    final int subtaskId = in.readInt();
                    final int transactionIdInPool = in.readInt();
                    final int idPoolRangeStartId = in.readInt();
                    final int idPoolRangeEndIdExclusive = in.readInt();
                    return ConfluentKafkaCommittableV1.fromRestoredState(
                            transactionIdString,
                            transactionIdPrefix,
                            subtaskId,
                            transactionIdInPool,
                            idPoolRangeStartId,
                            idPoolRangeEndIdExclusive);
                case KafkaCommittableV1.VERSION:
                    final short epoch = in.readShort();
                    final long producerId = in.readLong();
                    final String transactionalId = in.readUTF();
                    return new KafkaCommittableV1(producerId, epoch, transactionalId, null);
                default:
                    throw new RuntimeException("Unexpected KafkaCommittable version: " + version);
            }
        }
    }
}
