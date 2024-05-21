/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

/** Version 2 of {@link KafkaCommittable}, which only contains the {@code transactional.id}. */
@Confluent
@Internal
public class ConfluentKafkaCommittableV1 implements KafkaCommittable {

    /** Offset the version by 10000 to avoid potential clashes when merging back from OSS Flink. */
    public static final int VERSION = 10001;

    private final String transactionalIdString;
    private final String transactionalIdPrefix;
    private final int subtaskId;
    private final int transactionIdInPool;
    private final int idPoolRangeStartId;
    private final int idPoolRangeEndIdExclusive;

    @Nullable private Recyclable<? extends InternalKafkaProducer<?, ?>> producer;

    public static ConfluentKafkaCommittableV1 tryCast(KafkaCommittable committable) {
        try {
            return (ConfluentKafkaCommittableV1) committable;
        } catch (ClassCastException e) {
            throw new RuntimeException(
                    "Expected a ConfluentKafkaCommittableV1, but got " + committable, e);
        }
    }

    public static ConfluentKafkaCommittableV1 fromRestoredState(
            String transactionalId,
            String transactionalIdPrefix,
            int subtaskId,
            int transactionIdInPool,
            int idPoolRangeStartId,
            int idPoolRangeEndIdExclusive) {
        return new ConfluentKafkaCommittableV1(
                transactionalId,
                null,
                transactionalIdPrefix,
                subtaskId,
                transactionIdInPool,
                idPoolRangeStartId,
                idPoolRangeEndIdExclusive);
    }

    public static ConfluentKafkaCommittableV1 of(
            String transactionalIdPrefix, int subtaskId, int transactionIdInPool) {
        return new ConfluentKafkaCommittableV1(
                TransactionalIdFactory.buildTransactionalId(
                        transactionalIdPrefix, subtaskId, transactionIdInPool),
                null,
                transactionalIdPrefix,
                subtaskId,
                transactionIdInPool);
    }

    public static ConfluentKafkaCommittableV1 applyRecyclableProducer(
            ConfluentKafkaCommittableV1 committable,
            Recyclable<? extends InternalKafkaProducer<?, ?>> recyclableProducer) {
        return new ConfluentKafkaCommittableV1(
                committable.getTransactionalId(),
                recyclableProducer,
                committable.getTransactionalIdPrefix(),
                committable.getSubtaskId(),
                committable.getTransactionIdInPool());
    }

    private ConfluentKafkaCommittableV1(
            String transactionalId,
            @Nullable Recyclable<? extends InternalKafkaProducer<?, ?>> producer,
            String transactionalIdPrefix,
            int subtaskId,
            int transactionIdInPool) {
        this(
                transactionalId,
                producer,
                transactionalIdPrefix,
                subtaskId,
                transactionIdInPool,
                leftOrRightRangeStartId(transactionIdInPool),
                leftOrRightRangeEndIdExclusive(transactionIdInPool));
    }

    private static int leftOrRightRangeStartId(int transactionIdInPool) {
        if (TransactionIdRangeState.isLeftRange(transactionIdInPool)) {
            return TransactionIdRangeState.LEFT_RANGE_START_ID;
        } else if (TransactionIdRangeState.isRightRange(transactionIdInPool)) {
            return TransactionIdRangeState.RIGHT_RANGE_START_ID;
        }
        throw new IllegalStateException(
                "Invalid transaction id within pool: " + transactionIdInPool);
    }

    private static int leftOrRightRangeEndIdExclusive(int transactionIdInPool) {
        if (TransactionIdRangeState.isLeftRange(transactionIdInPool)) {
            return TransactionIdRangeState.LEFT_RANGE_END_ID_EXCLUSIVE;
        } else if (TransactionIdRangeState.isRightRange(transactionIdInPool)) {
            return TransactionIdRangeState.RIGHT_RANGE_END_ID_EXCLUSIVE;
        }
        throw new IllegalStateException(
                "Invalid transaction id within pool: " + transactionIdInPool);
    }

    private ConfluentKafkaCommittableV1(
            String transactionalId,
            @Nullable Recyclable<? extends InternalKafkaProducer<?, ?>> producer,
            String transactionalIdPrefix,
            int subtaskId,
            int transactionIdInPool,
            int idPoolRangeStartId,
            int idPoolRangeEndIdExclusive) {
        this.transactionalIdString = transactionalId;
        this.producer = producer;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.subtaskId = subtaskId;
        this.transactionIdInPool = transactionIdInPool;
        this.idPoolRangeStartId = idPoolRangeStartId;
        this.idPoolRangeEndIdExclusive = idPoolRangeEndIdExclusive;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public String getTransactionalId() {
        return transactionalIdString;
    }

    @Override
    public Optional<Recyclable<? extends InternalKafkaProducer<?, ?>>> getProducer() {
        return Optional.ofNullable(producer);
    }

    public String getTransactionalIdPrefix() {
        return transactionalIdPrefix;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getTransactionIdInPool() {
        return transactionIdInPool;
    }

    public int getIdPoolRangeStartId() {
        return idPoolRangeStartId;
    }

    public int getIdPoolRangeEndIdExclusive() {
        return idPoolRangeEndIdExclusive;
    }

    @Override
    public String toString() {
        return "ConfluentKafkaCommittableV1{"
                + "transactionalIdString='"
                + transactionalIdString
                + '\''
                + ", transactionalIdPrefix='"
                + transactionalIdPrefix
                + '\''
                + ", subtaskId="
                + subtaskId
                + ", transactionIdInPool="
                + transactionIdInPool
                + ", idPoolRangeStartId="
                + idPoolRangeStartId
                + ", idPoolRangeEndIdExclusive="
                + idPoolRangeEndIdExclusive
                + ", producer="
                + producer
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
        ConfluentKafkaCommittableV1 that = (ConfluentKafkaCommittableV1) o;
        return subtaskId == that.subtaskId
                && transactionIdInPool == that.transactionIdInPool
                && idPoolRangeStartId == that.idPoolRangeStartId
                && idPoolRangeEndIdExclusive == that.idPoolRangeEndIdExclusive
                && Objects.equals(transactionalIdString, that.transactionalIdString)
                && Objects.equals(transactionalIdPrefix, that.transactionalIdPrefix)
                && Objects.equals(producer, that.producer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                transactionalIdString,
                transactionalIdPrefix,
                subtaskId,
                transactionIdInPool,
                idPoolRangeStartId,
                idPoolRangeEndIdExclusive,
                producer);
    }
}
