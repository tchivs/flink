/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.connector.kafka.sink.testutils.TestKafkaProducerFactory;

import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Test for {@link FixedSizeProducerPool}. */
public class FixedSizeProducerPoolTest {
    private static final String TEST_TXN_PREFIX = "txn-prefix";

    @Test
    public void testLeftRangePool() {
        final FixedSizeProducerPool<String, String> firstSubtaskPool =
                poolForSubtask(0, TransactionIdRangeState.ofLeftRange(3));
        acquireAndValidateAllRemainingProducersInPool(
                firstSubtaskPool,
                0,
                TransactionIdRangeState.FIXED_POOL_SIZE,
                TransactionIdRangeState.LEFT_RANGE_START_ID);

        final FixedSizeProducerPool<String, String> secondSubtaskPool =
                poolForSubtask(1, TransactionIdRangeState.ofLeftRange(3));
        acquireAndValidateAllRemainingProducersInPool(
                secondSubtaskPool,
                1,
                TransactionIdRangeState.FIXED_POOL_SIZE,
                TransactionIdRangeState.LEFT_RANGE_START_ID);

        final FixedSizeProducerPool<String, String> thirdSubtaskPool =
                poolForSubtask(2, TransactionIdRangeState.ofLeftRange(3));
        acquireAndValidateAllRemainingProducersInPool(
                thirdSubtaskPool,
                2,
                TransactionIdRangeState.FIXED_POOL_SIZE,
                TransactionIdRangeState.LEFT_RANGE_START_ID);
    }

    @Test
    public void testRightRangePool() {
        final FixedSizeProducerPool<String, String> firstSubtaskPool =
                poolForSubtask(0, TransactionIdRangeState.ofRightRange(3));
        acquireAndValidateAllRemainingProducersInPool(
                firstSubtaskPool,
                0,
                TransactionIdRangeState.FIXED_POOL_SIZE,
                TransactionIdRangeState.RIGHT_RANGE_START_ID);

        final FixedSizeProducerPool<String, String> secondSubtaskPool =
                poolForSubtask(1, TransactionIdRangeState.ofRightRange(3));
        acquireAndValidateAllRemainingProducersInPool(
                secondSubtaskPool,
                1,
                TransactionIdRangeState.FIXED_POOL_SIZE,
                TransactionIdRangeState.RIGHT_RANGE_START_ID);

        final FixedSizeProducerPool<String, String> thirdSubtaskPool =
                poolForSubtask(2, TransactionIdRangeState.ofRightRange(3));
        acquireAndValidateAllRemainingProducersInPool(
                thirdSubtaskPool,
                2,
                TransactionIdRangeState.FIXED_POOL_SIZE,
                TransactionIdRangeState.RIGHT_RANGE_START_ID);
    }

    @Test
    public void testRecycleProducer() {
        final FixedSizeProducerPool<String, String> pool =
                poolForSubtask(0, TransactionIdRangeState.ofLeftRange(1));
        final InternalKafkaProducer<String, String> producer = pool.getForCheckpoint(10);

        pool.recycle(producer);
        assertThat(pool.numProducers()).isEqualTo(1);

        final InternalKafkaProducer<String, String> nextProducer = pool.getForCheckpoint(11);
        // since the previous producer was recycled, the pool should not be creating a new producer
        assertSame(producer, nextProducer);

        // remaining producers in pool should continue to be acquirable
        acquireAndValidateAllRemainingProducersInPool(
                pool,
                0,
                TransactionIdRangeState.FIXED_POOL_SIZE - 1,
                TransactionIdRangeState.LEFT_RANGE_START_ID + 1);
    }

    private static FixedSizeProducerPool<String, String> poolForSubtask(
            int subtaskIndex, TransactionIdRangeState transactionIdRange) {
        return new FixedSizeProducerPool<>(
                transactionIdRange,
                TEST_TXN_PREFIX,
                subtaskIndex,
                new TestKafkaProducerFactory(new HashSet<>(), new HashSet<>()));
    }

    private static void acquireAndValidateAllRemainingProducersInPool(
            FixedSizeProducerPool<String, String> testPool,
            int subtaskIndex,
            int remainingPoolSize,
            int remainingTransactionIdsOffset) {
        for (int i = 0; i < remainingPoolSize; i++) {
            final InternalKafkaProducer<String, String> nextProducer = testPool.getForCheckpoint(i);
            assertThat(nextProducer.getAssignedCommittable())
                    .isEqualTo(
                            ConfluentKafkaCommittableV1.of(
                                    TEST_TXN_PREFIX,
                                    subtaskIndex,
                                    remainingTransactionIdsOffset + i));
        }

        // after pool size is depleted, the pool should no longer offer producers
        assertThrows(
                "pool has been depleted",
                RuntimeException.class,
                () -> testPool.getForCheckpoint(Long.MAX_VALUE));
    }
}
