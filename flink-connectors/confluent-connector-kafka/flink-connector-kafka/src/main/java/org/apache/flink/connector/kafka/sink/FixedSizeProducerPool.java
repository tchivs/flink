/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TransactionalProducerPool} implementation that only offers up to a given amount of
 * {@link InternalKafkaProducer} instances to be used for Flink checkpoints. The maximum amount is
 * equal to the number of transaction IDs that are available for use within the current execution's
 * {@link TransactionIdRangeState}.
 *
 * <p>Each producer instance offered by the pool is statically assigned a transaction ID. Once the
 * offered producer finishes its transaction (i.e. committed or aborted), it may be recycled back to
 * the pool for use by future Flink checkpoints.
 */
@Confluent
@Internal
public class FixedSizeProducerPool<K, V> implements TransactionalProducerPool<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(FixedSizeProducerPool.class);

    private final Deque<InternalKafkaProducer<K, V>> idleProducerPool;
    private final TransactionIdRangeState currentExecutionTransactionIdRange;
    private int numCreatedProducers;
    private final String transactionalIdPrefix;
    private final int subtaskIndex;
    private final InternalKafkaProducerFactory<K, V, ?> producerFactory;

    public FixedSizeProducerPool(
            TransactionIdRangeState currentExecutionTransactionIdRange,
            String transactionalIdPrefix,
            int subtaskIndex,
            InternalKafkaProducerFactory<K, V, ?> producerFactory) {
        this.producerFactory = producerFactory;
        this.idleProducerPool = new ArrayDeque<>(currentExecutionTransactionIdRange.getPoolSize());
        this.currentExecutionTransactionIdRange = currentExecutionTransactionIdRange;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.subtaskIndex = subtaskIndex;
        this.numCreatedProducers = 0;
    }

    @Override
    public InternalKafkaProducer<K, V> getForCheckpoint(long checkpointId) {
        final InternalKafkaProducer<K, V> reusableInactiveProducer = idleProducerPool.poll();
        if (reusableInactiveProducer != null) {
            LOG.info(
                    "Using recycled producer with assigned KafkaCommittable {} (within range {}) for checkpoint {} of subtask {}.",
                    reusableInactiveProducer.getAssignedCommittable(),
                    currentExecutionTransactionIdRange,
                    checkpointId,
                    subtaskIndex);
            return reusableInactiveProducer;
        }
        if (numCreatedProducers >= currentExecutionTransactionIdRange.getPoolSize()) {
            throw new RuntimeException(
                    "Transaction id pool has been depleted, and all producer instances are pending commit. Consider increasing the pool size.");
        }

        final InternalKafkaProducer<K, V> newProducer =
                producerFactory.createTransactional(
                        transactionalIdPrefix,
                        subtaskIndex,
                        currentExecutionTransactionIdRange.getStartId() + numCreatedProducers);
        newProducer.initTransactions(false);
        numCreatedProducers++;

        LOG.info(
                "Created new producer with assigned KafkaCommittable {} (within range {}) for checkpoint {} of subtask {}.",
                newProducer.getAssignedCommittable(),
                currentExecutionTransactionIdRange,
                checkpointId,
                subtaskIndex);
        return newProducer;
    }

    @Override
    public void recycle(InternalKafkaProducer<K, V> committedProducer) {
        checkState(
                !committedProducer.isInTransaction(),
                "A producer that is still in a transaction should not be recycled.");
        idleProducerPool.add(committedProducer);
    }

    @Override
    public int numProducers() {
        return idleProducerPool.size();
    }

    @Override
    public void clearAll() {
        idleProducerPool.clear();
    }
}
