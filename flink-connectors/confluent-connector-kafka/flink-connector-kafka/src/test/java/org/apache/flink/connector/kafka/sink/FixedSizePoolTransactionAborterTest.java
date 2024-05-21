/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.connector.kafka.sink.testutils.TestKafkaProducerFactory;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FixedSizePoolTransactionAborter}. */
public class FixedSizePoolTransactionAborterTest {

    private static final int CURRENT_NUM_SUBTASKS = 3;
    private static final String TEST_TXN_PREFIX = "txn-prefix";

    @Test
    public void testAbortLeftRange() {
        // simulate fresh execution, where no transaction ids were used before
        final Set<String> usedTransactionIds = new HashSet<>();

        // for tracking aborted transaction ids
        final Set<String> trackAbortedTransactionIds = new HashSet<>();

        final TransactionIdRangeState idRange =
                TransactionIdRangeState.ofLeftRange(CURRENT_NUM_SUBTASKS);
        for (int subtaskIndex = 0; subtaskIndex < CURRENT_NUM_SUBTASKS; subtaskIndex++) {
            final FixedSizePoolTransactionAborter subtaskAborter =
                    aborterForSubtask(
                            subtaskIndex, idRange, usedTransactionIds, trackAbortedTransactionIds);
            subtaskAborter.abortLingeringTransactions();
        }

        final Set<String> expectedAbortedTransactionIds = new HashSet<>();
        for (int subtaskIndex = 0; subtaskIndex < CURRENT_NUM_SUBTASKS; subtaskIndex++) {
            expectedAbortedTransactionIds.addAll(
                    subtaskTransactionIdsForRange(
                            subtaskIndex, idRange.getStartId(), idRange.getStartId()));
        }
        assertThat(trackAbortedTransactionIds).isEqualTo(expectedAbortedTransactionIds);
    }

    @Test
    public void testAbortRightRange() {
        // simulate fresh execution, where no transaction ids were used before
        final Set<String> usedTransactionIds = new HashSet<>();

        // for tracking aborted transaction ids
        final Set<String> trackAbortedTransactionIds = new HashSet<>();

        final TransactionIdRangeState rightRange =
                TransactionIdRangeState.ofRightRange(CURRENT_NUM_SUBTASKS);
        for (int subtaskIndex = 0; subtaskIndex < CURRENT_NUM_SUBTASKS; subtaskIndex++) {
            final FixedSizePoolTransactionAborter subtaskAborter =
                    aborterForSubtask(
                            subtaskIndex,
                            rightRange,
                            usedTransactionIds,
                            trackAbortedTransactionIds);
            subtaskAborter.abortLingeringTransactions();
        }

        final Set<String> expectedAbortedTransactionIds = new HashSet<>();
        for (int subtaskIndex = 0; subtaskIndex < CURRENT_NUM_SUBTASKS; subtaskIndex++) {
            expectedAbortedTransactionIds.addAll(
                    subtaskTransactionIdsForRange(
                            subtaskIndex, rightRange.getStartId(), rightRange.getStartId()));
        }
        assertThat(trackAbortedTransactionIds).isEqualTo(expectedAbortedTransactionIds);
    }

    private static FixedSizePoolTransactionAborter aborterForSubtask(
            int subtaskIndex,
            TransactionIdRangeState rangeToAbort,
            Set<String> usedTransactionIds,
            Set<String> trackAbortedTransactionIds) {
        return new FixedSizePoolTransactionAborter(
                TEST_TXN_PREFIX,
                subtaskIndex,
                null,
                rangeToAbort,
                new TestKafkaProducerFactory(usedTransactionIds, trackAbortedTransactionIds));
    }

    private static Set<String> subtaskTransactionIdsForRange(
            int subtaskIndex, int startId, int endId) {
        final Set<String> expected = new HashSet<>();
        for (int i = startId; i <= endId; i++) {
            expected.add(
                    TransactionalIdFactory.buildTransactionalId(TEST_TXN_PREFIX, subtaskIndex, i));
        }
        return expected;
    }
}
