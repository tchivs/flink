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

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/**
 * This class holds the necessary information to construct a new {@link FlinkKafkaInternalProducer}
 * to commit transactions in {@link KafkaCommitter}.
 */
class KafkaCommittableV1 implements KafkaCommittable {

    private final long producerId;
    private final short epoch;
    private final String transactionalId;
    @Nullable private Recyclable<? extends InternalKafkaProducer<?, ?>> producer;

    public static KafkaCommittableV1 tryCast(KafkaCommittable committable) {
        try {
            return (KafkaCommittableV1) committable;
        } catch (ClassCastException e) {
            throw new RuntimeException("Expected a KafkaCommittableV1, but got " + committable, e);
        }
    }

    public KafkaCommittableV1(
            long producerId,
            short epoch,
            String transactionalId,
            @Nullable Recyclable<? extends InternalKafkaProducer<?, ?>> producer) {
        this.producerId = producerId;
        this.epoch = epoch;
        this.transactionalId = transactionalId;
        this.producer = producer;
    }

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    public String getTransactionalId() {
        return transactionalId;
    }

    public Optional<Recyclable<? extends InternalKafkaProducer<?, ?>>> getProducer() {
        return Optional.ofNullable(producer);
    }

    @Override
    public String toString() {
        return "KafkaCommittable{"
                + "producerId="
                + producerId
                + ", epoch="
                + epoch
                + ", transactionalId="
                + transactionalId
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
        KafkaCommittableV1 that = (KafkaCommittableV1) o;
        return producerId == that.producerId
                && epoch == that.epoch
                && transactionalId.equals(that.transactionalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, epoch, transactionalId);
    }
}
