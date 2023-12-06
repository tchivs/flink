/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransactionalProducerPool} that provides producer instances with ever-increasing
 * transactional.ids of format {@code userPrefix-subtaskId-checkpointId}.
 */
@Confluent
public class IncreasingIdsProducerPool<K, V> implements TransactionalProducerPool<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(IncreasingIdsProducerPool.class);

    private final Deque<FlinkKafkaInternalProducer<K, V>> pool = new ArrayDeque<>();
    private final String transactionalIdPrefix;
    private final int subtaskIndex;
    private final JavaReflectionProducerFactory<K, V> producerFactory;

    private long lastCheckpointId;

    IncreasingIdsProducerPool(
            String transactionalIdPrefix,
            JavaReflectionProducerFactory<K, V> producerFactory,
            Sink.InitContext sinkInitContext) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix);
        this.producerFactory = checkNotNull(producerFactory);
        this.subtaskIndex = sinkInitContext.getSubtaskId();
        this.lastCheckpointId =
                sinkInitContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
    }

    @Override
    public InternalKafkaProducer<K, V> getForCheckpoint(long checkpointId) {
        checkState(
                checkpointId > lastCheckpointId,
                "Expected %s > %s",
                checkpointId,
                lastCheckpointId);
        FlinkKafkaInternalProducer<K, V> producer = null;
        // in case checkpoints have been aborted, Flink would create non-consecutive transaction ids
        // this loop ensures that all gaps are filled with initialized (empty) transactions
        for (long id = lastCheckpointId + 1; id <= checkpointId; id++) {
            String transactionalId =
                    TransactionalIdFactory.buildTransactionalId(
                            transactionalIdPrefix, subtaskIndex, id);
            producer = getOrCreateTransactionalProducer(transactionalId);
        }
        this.lastCheckpointId = checkpointId;
        assert producer != null;
        LOG.info("Created new transactional producer {}", producer.getTransactionalId());
        return producer;
    }

    @Override
    public int numProducers() {
        return pool.size();
    }

    @Override
    public void recycle(InternalKafkaProducer<K, V> committedProducer) {
        checkState(
                !committedProducer.isInTransaction(),
                "A producer that is still in a transaction should not be recycled.");
        pool.add(tryCast(committedProducer));
    }

    @Override
    public void clearAll() {
        pool.clear();
    }

    private FlinkKafkaInternalProducer<K, V> getOrCreateTransactionalProducer(
            String transactionalId) {
        FlinkKafkaInternalProducer<K, V> producer = pool.poll();
        if (producer == null) {
            producer = producerFactory.createTransactional(transactionalId);
            producer.initTransactions();
        } else {
            producer.initTransactionId(transactionalId);
        }
        return producer;
    }

    private static <K, V> FlinkKafkaInternalProducer<K, V> tryCast(
            InternalKafkaProducer<K, V> producer) {
        try {
            return (FlinkKafkaInternalProducer<K, V>) producer;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Expected internal producers of type " + FlinkKafkaInternalProducer.class);
        }
    }
}
