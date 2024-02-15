/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Indicates the range of transaction ids used by the current execution. This is persisted as global
 * state of the Kafka sink, so that future restored executions have information about this. In
 * restored executions, the previous execution's {@link TransactionIdRangeState} indicate the range
 * of transaction ids that needs to be committed. Any other ids outside of this range that still
 * have ongoing transactions are considered to be lingering and should be aborted.
 *
 * <p>There is only two possible non-overlapping ranges that any given execution attempt will be
 * actively using for its Kafka transactions:
 *
 * <ul>
 *   <li>{@link #ofLeftRange(int)}, which is [0, FIXED_POOL_SIZE).
 *   <li>{@link #ofRightRange(int)}, which is [FIXED_POOL_SIZE, 2*FIXED_POOL_SIZE).
 * </ul>
 *
 * <p>Across consecutive execution attempts, we alternate between {@code LEFT_RANGE} and {@code
 * RIGHT_RANGE} depending on the range used by the previous execution. If the previous execution was
 * using {@code LEFT_RANGE}, the current execution will use {@code RIGHT_RANGE}, and vice versa.
 * This alternation between the two ranges avoids race condition across subtasks, specifically in
 * rescaling scenarios, where one subtask is attempting to clean up and use a transaction ID which
 * should actually be committed by a restore producer on another subtask. The decision to alternate
 * between ranges is implemented by {@link
 * #alternateRangeForCurrentExecution(TransactionIdRangeState, int)}, which should be used by the
 * {@link KafkaWriter} to determine which range it should use for the current execution.
 */
@Confluent
@Internal
public class TransactionIdRangeState {

    /** Offset the version by 10000 to avoid potential clashes when merging back from OSS Flink. */
    public static final int VERSION = 10001;

    /**
     * This value cannot be changed and must remain stable across executions.
     *
     * <p>TODO: this fixed value is enough for CCloud Flink since we currently do not enable
     * concurrent checkpoints (and also do not plan to in the future). We should make this
     * configurable as a follow-up when we open-source this.
     */
    public static final int FIXED_POOL_SIZE = 5;

    public static final int LEFT_RANGE_START_ID = 0;
    public static final int LEFT_RANGE_END_ID_EXCLUSIVE = LEFT_RANGE_START_ID + FIXED_POOL_SIZE;

    public static final int RIGHT_RANGE_START_ID = FIXED_POOL_SIZE;
    public static final int RIGHT_RANGE_END_ID_EXCLUSIVE = RIGHT_RANGE_START_ID + FIXED_POOL_SIZE;

    /** Transaction range: [0, FIXED_POOL_SIZE). */
    public static TransactionIdRangeState ofLeftRange(int numSubtasks) {
        return new TransactionIdRangeState(LEFT_RANGE_START_ID, FIXED_POOL_SIZE, numSubtasks);
    }

    /** Transaction range: [FIXED_POOL_SIZE, 2 * FIXED_POOL_SIZE). */
    public static TransactionIdRangeState ofRightRange(int numSubtasks) {
        return new TransactionIdRangeState(RIGHT_RANGE_START_ID, FIXED_POOL_SIZE, numSubtasks);
    }

    private final int startId;
    private final int poolSize;
    private final int numSubtasks;

    public static TransactionIdRangeState alternateRangeForCurrentExecution(
            @Nullable TransactionIdRangeState previousRestoredRange, int currentNumSubtasks) {
        if (previousRestoredRange == null) {
            return ofLeftRange(currentNumSubtasks);
        }

        return isLeftRange(previousRestoredRange)
                ? ofRightRange(currentNumSubtasks)
                : ofLeftRange(currentNumSubtasks);
    }

    public static TransactionIdRangeState fromRestoredState(
            int startCommittableTransactionId, int poolSize, int numSubtasks) {
        final TransactionIdRangeState restoredRange =
                new TransactionIdRangeState(startCommittableTransactionId, poolSize, numSubtasks);
        if (!isLeftRange(restoredRange) && !isRightRange(restoredRange)) {
            throw new IllegalStateException(
                    "Invalid restored TransactionsIdRangeGlobalState: " + restoredRange + ".");
        }
        return restoredRange;
    }

    private TransactionIdRangeState(int startId, int poolSize, int numSubtasks) {
        this.startId = startId;
        this.poolSize = poolSize;
        this.numSubtasks = numSubtasks;
    }

    public static boolean isLeftRange(TransactionIdRangeState range) {
        return range.getStartId() == LEFT_RANGE_START_ID && range.getPoolSize() == FIXED_POOL_SIZE;
    }

    public static boolean isRightRange(TransactionIdRangeState range) {
        return range.getStartId() == RIGHT_RANGE_START_ID && range.getPoolSize() == FIXED_POOL_SIZE;
    }

    public static boolean isLeftRange(int id) {
        return id >= LEFT_RANGE_START_ID && id < LEFT_RANGE_END_ID_EXCLUSIVE;
    }

    public static boolean isRightRange(int id) {
        return id >= RIGHT_RANGE_START_ID && id < RIGHT_RANGE_END_ID_EXCLUSIVE;
    }

    public int getStartId() {
        return startId;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public int getNumSubtasks() {
        return numSubtasks;
    }

    @Override
    public String toString() {
        return "TransactionIdRangeState{"
                + "startId="
                + startId
                + ", poolSize="
                + poolSize
                + ", numSubtasks="
                + numSubtasks
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
        TransactionIdRangeState that = (TransactionIdRangeState) o;
        return startId == that.startId
                && poolSize == that.poolSize
                && numSubtasks == that.numSubtasks;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startId, poolSize, numSubtasks);
    }
}
