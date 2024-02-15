/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink.testutils;

import org.apache.flink.connector.kafka.sink.ConfluentKafkaCommittableV1;
import org.apache.flink.connector.kafka.sink.InternalKafkaProducer;
import org.apache.flink.connector.kafka.sink.KafkaCommittable;

import org.apache.kafka.clients.producer.MockProducer;

import java.util.Set;
import java.util.function.Consumer;

/** Mock implementation of {@link InternalKafkaProducer}. */
public class TestKafkaProducer extends MockProducer<String, String>
        implements InternalKafkaProducer<String, String> {

    private final ConfluentKafkaCommittableV1 committable;
    private final Set<String> abortedTransactionIdsOnInit;
    private final Set<String> usedTransactionIds;

    public TestKafkaProducer(
            ConfluentKafkaCommittableV1 committable,
            Set<String> usedTransactionIds,
            Set<String> abortedTransactionIdsOnInit) {
        this.committable = committable;
        this.usedTransactionIds = usedTransactionIds;
        this.abortedTransactionIdsOnInit = abortedTransactionIdsOnInit;
    }

    @Override
    public KafkaCommittable getAssignedCommittable() {
        return committable;
    }

    @Override
    public KafkaCommittable prepareTransaction(
            Consumer<InternalKafkaProducer<String, String>> recycler) {
        return null;
    }

    @Override
    public void initTransactions() {
        usedTransactionIds.add(committable.getTransactionalId());
        abortedTransactionIdsOnInit.add(committable.getTransactionalId());
    }

    @Override
    public void initTransactions(boolean keepPreparedTransactions) {
        usedTransactionIds.add(committable.getTransactionalId());
        if (!keepPreparedTransactions) {
            abortedTransactionIdsOnInit.add(committable.getTransactionalId());
        }
    }

    @Override
    public boolean initAndAbortOngoingTransaction() {
        abortedTransactionIdsOnInit.add(committable.getTransactionalId());
        return !usedTransactionIds.add(committable.getTransactionalId());
    }

    @Override
    public void resumePreparedTransaction(KafkaCommittable restoredCommittable) {}

    @Override
    public boolean isInTransaction() {
        return false;
    }

    @Override
    public boolean hasRecordsInTransaction() {
        return false;
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
