/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * {@link TransactionAborter} for lingering transactions under the fixed-size transaction id pooling
 * scheme.
 */
@Confluent
@Internal
public class FixedSizePoolTransactionAborter extends TransactionAborter {
    private final TransactionIdRangeState previousTransactionIdRange;
    private final TransactionIdRangeState currentTransactionIdRange;

    private final InternalKafkaProducerFactory<?, ?, ?> producerFactory;

    public FixedSizePoolTransactionAborter(
            String transactionalIdPrefix,
            int subtaskId,
            @Nullable TransactionIdRangeState previousTransactionIdRange,
            TransactionIdRangeState currentTransactionIdRange,
            InternalKafkaProducerFactory<?, ?, ?> producerFactory) {
        super(
                transactionalIdPrefix,
                subtaskId,
                currentTransactionIdRange.getNumSubtasks(),
                producerFactory);
        this.previousTransactionIdRange = previousTransactionIdRange;
        this.currentTransactionIdRange = currentTransactionIdRange;
        this.producerFactory = producerFactory;
    }

    @Override
    protected void abortLingeringTransactions() {
        abortTransactionsForRange(
                currentTransactionIdRange.getStartId(),
                currentTransactionIdRange.getStartId() + currentTransactionIdRange.getPoolSize());
    }

    private void abortTransactionsForRange(int startId, int endIdExclusive) {
        if (previousTransactionIdRange != null
                && currentTransactionIdRange.getNumSubtasks()
                        >= previousTransactionIdRange.getNumSubtasks()) {
            // if we scaled up or remained the same parallelism for this execution, aborting
            // transactions of currently running subtasks will cover all transactions in previous
            // execution
            abortSubtaskTransactionsForRange(subtaskIndex, startId, endIdExclusive);
        } else {
            // this loop covers downscale scenarios; for example, downscale from p=6 to p=2
            // - subtask 0 will abort transactions of subtasks 0, 2, 4 in any previous execution
            // - subtask 1 will abort transactions of subtasks 1, 3, 5 in any previous execution
            for (int subtaskToAbort = subtaskIndex; ; subtaskToAbort += numSubtasks) {
                if (abortSubtaskTransactionsForRange(subtaskToAbort, startId, endIdExclusive)
                        == 0) {
                    break;
                }
            }
        }
    }

    private int abortSubtaskTransactionsForRange(
            int subtaskIndex, int startId, int endIdExclusive) {
        int numTransactionAborted = 0;
        for (int i = startId; i < endIdExclusive; i++) {
            try (final InternalKafkaProducer<?, ?> producer =
                    producerFactory.createTransactional(transactionalIdPrefix, subtaskIndex, i)) {
                if (!producer.initAndAbortOngoingTransaction()) {
                    break;
                }
                numTransactionAborted++;
            }
        }
        return numTransactionAborted;
    }

    @Override
    public void close() {}
}
