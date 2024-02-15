/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.kafka.sink.TransactionIdRangeState.FIXED_POOL_SIZE;
import static org.apache.flink.connector.kafka.sink.TransactionIdRangeState.LEFT_RANGE_START_ID;
import static org.apache.flink.connector.kafka.sink.TransactionIdRangeState.RIGHT_RANGE_START_ID;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TransactionIdRangeState}, especially how ranges alternate across executions to
 * be non-overlapping.
 */
public class TransactionIdRangeStateTest {

    @Test
    public void testAlternateRangeFromLeftForCurrentExecution() {
        final TransactionIdRangeState previousExecutionRange =
                TransactionIdRangeState.ofLeftRange(1);
        assertThat(
                        TransactionIdRangeState.alternateRangeForCurrentExecution(
                                previousExecutionRange, 3))
                .isEqualTo(TransactionIdRangeState.ofRightRange(3));
    }

    @Test
    public void testAlternateRangeFromRightForCurrentExecution() {
        final TransactionIdRangeState previousExecutionRange =
                TransactionIdRangeState.ofRightRange(1);
        assertThat(
                        TransactionIdRangeState.alternateRangeForCurrentExecution(
                                previousExecutionRange, 3))
                .isEqualTo(TransactionIdRangeState.ofLeftRange(3));
    }

    @Test
    public void testUseLeftRangeIfNoPreviousRange() {
        assertThat(TransactionIdRangeState.alternateRangeForCurrentExecution(null, 1))
                .isEqualTo(TransactionIdRangeState.ofLeftRange(1));
    }

    @Test
    public void testLeftRightRangeDoesNoOverlap() {
        final int leftRangeLargestId = LEFT_RANGE_START_ID + FIXED_POOL_SIZE - 1;
        assertThat(leftRangeLargestId < RIGHT_RANGE_START_ID).isTrue();
    }
}
