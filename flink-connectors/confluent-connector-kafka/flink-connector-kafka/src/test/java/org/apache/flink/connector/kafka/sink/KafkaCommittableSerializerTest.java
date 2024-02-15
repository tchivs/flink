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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for serializing and deserializing {@link ConfluentKafkaCommittableV1} with {@link
 * KafkaCommittableSerializer}.
 */
public class KafkaCommittableSerializerTest extends TestLogger {

    private static final KafkaCommittableSerializer SERIALIZER = new KafkaCommittableSerializer();

    @Test
    public void testCommittableSerDe() throws IOException {
        final ConfluentKafkaCommittableV1 committable =
                ConfluentKafkaCommittableV1.of("txn-prefix", 0, 3);
        final byte[] serialized = SERIALIZER.serialize(committable);
        assertThat(SERIALIZER.deserialize(ConfluentKafkaCommittableV1.VERSION, serialized))
                .isEqualTo(committable);
    }

    @Test
    public void testV1BackwardsCompatibility() throws IOException {
        final String transactionalId =
                TransactionalIdFactory.buildTransactionalId("txn-prefix", 0, 3);
        final long producerId = 1L;
        final short epoch = 5;
        final KafkaCommittableV1 committableV1 =
                new KafkaCommittableV1(producerId, epoch, transactionalId, null);
        final byte[] serialized = serializeV1(committableV1);
        assertThat(SERIALIZER.deserialize(KafkaCommittableV1.VERSION, serialized))
                .isEqualTo(committableV1);
    }

    /**
     * Serialization for historical binary formats.
     *
     * <p>Moved here from {@link KafkaCommittableSerializer} after a version is retired.
     */
    private static byte[] serializeV1(KafkaCommittableV1 committableV1) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeShort(committableV1.getEpoch());
            out.writeLong(committableV1.getProducerId());
            out.writeUTF(committableV1.getTransactionalId());
            out.flush();
            return baos.toByteArray();
        }
    }
}
