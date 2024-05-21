/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;

import org.apache.kafka.clients.producer.Producer;

import java.util.function.Consumer;

/**
 * Internal interface that extends Kafka's {@link Producer} interface to additionally have 2PC
 * support for preparing transactions to obtain committables, as well as resuming prepared
 * transactions with committables.
 *
 * @param <K> type of key
 * @param <V> type of value
 */
@Confluent
public interface InternalKafkaProducer<K, V> extends Producer<K, V> {

    KafkaCommittable getAssignedCommittable();

    KafkaCommittable prepareTransaction(Consumer<InternalKafkaProducer<K, V>> recycler);

    boolean initAndAbortOngoingTransaction();

    void resumePreparedTransaction(KafkaCommittable restoredCommittable);

    boolean isInTransaction();

    boolean hasRecordsInTransaction();

    boolean isClosed();
}
