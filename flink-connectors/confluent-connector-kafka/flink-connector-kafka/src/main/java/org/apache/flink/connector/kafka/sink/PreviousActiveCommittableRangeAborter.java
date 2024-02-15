/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Keeps track of committables for previous execution's subtasks that are restored to current
 * subtask.
 */
@Confluent
@Internal
final class PreviousActiveCommittableRangeAborter {

    private final Set<Integer> restoredSubtaskIds;
    private final Consumer<ConfluentKafkaCommittableV1> transactionIdAborter;
    private final Map<Integer, Set<ConfluentKafkaCommittableV1>> seenRestoredCommittables =
            new HashMap<>();

    // keep track of previous execution range and transaction id prefix; these should not change
    // across restored committables!
    private Integer previousActiveRangeStartId;
    private Integer previousActiveRangeEndIdExclusive;
    private String previousTransactionIdPrefix;

    PreviousActiveCommittableRangeAborter(
            Set<Integer> restoredSubtaskIds,
            Consumer<ConfluentKafkaCommittableV1> transactionIdAborter) {
        this.restoredSubtaskIds = restoredSubtaskIds;
        this.transactionIdAborter = transactionIdAborter;
    }

    void trackRestoredCommittable(ConfluentKafkaCommittableV1 restoredCommittable) {
        setOrCheckPreviousActiveRangeStartId(restoredCommittable);
        setOrCheckPreviousActiveRangeEndIdExclusive(restoredCommittable);
        setOrCheckPreviousTransactionIdPrefix(restoredCommittable);

        final int subtaskId = restoredCommittable.getSubtaskId();
        seenRestoredCommittables.computeIfAbsent(subtaskId, HashSet::new).add(restoredCommittable);
    }

    void abortNonCheckpointedCommittablesWithinActiveRange() {
        for (Map.Entry<Integer, Set<ConfluentKafkaCommittableV1>> seenRestoredSubtaskCommittables :
                seenRestoredCommittables.entrySet()) {
            final Set<Integer> checkpointedCommittablesWithinActiveRange =
                    seenRestoredSubtaskCommittables.getValue().stream()
                            .map(ConfluentKafkaCommittableV1::getTransactionIdInPool)
                            .collect(Collectors.toSet());

            for (int i = previousActiveRangeStartId; i < previousActiveRangeEndIdExclusive; i++) {
                if (!checkpointedCommittablesWithinActiveRange.contains(i)) {
                    transactionIdAborter.accept(
                            ConfluentKafkaCommittableV1.of(
                                    previousTransactionIdPrefix,
                                    seenRestoredSubtaskCommittables.getKey(),
                                    i));
                }
            }
        }
    }

    private void setOrCheckPreviousActiveRangeStartId(
            ConfluentKafkaCommittableV1 restoredCommittable) {
        if (previousActiveRangeStartId == null) {
            previousActiveRangeStartId = restoredCommittable.getIdPoolRangeStartId();
        } else if (previousActiveRangeStartId != restoredCommittable.getIdPoolRangeStartId()) {
            throw new IllegalStateException(
                    String.format(
                            "Restored committables with different active range start ids: %s and %s.",
                            previousActiveRangeStartId,
                            restoredCommittable.getIdPoolRangeStartId()));
        }
    }

    private void setOrCheckPreviousActiveRangeEndIdExclusive(
            ConfluentKafkaCommittableV1 restoredCommittable) {
        if (previousActiveRangeEndIdExclusive == null) {
            previousActiveRangeEndIdExclusive = restoredCommittable.getIdPoolRangeEndIdExclusive();
        } else if (previousActiveRangeEndIdExclusive
                != restoredCommittable.getIdPoolRangeEndIdExclusive()) {
            throw new IllegalStateException(
                    String.format(
                            "Restored committables with different active range end ids: %s and %s.",
                            previousActiveRangeEndIdExclusive,
                            restoredCommittable.getIdPoolRangeEndIdExclusive()));
        }
    }

    private void setOrCheckPreviousTransactionIdPrefix(
            ConfluentKafkaCommittableV1 restoredCommittable) {
        if (previousTransactionIdPrefix == null) {
            previousTransactionIdPrefix = restoredCommittable.getTransactionalIdPrefix();
        } else if (!previousTransactionIdPrefix.equals(
                restoredCommittable.getTransactionalIdPrefix())) {
            throw new IllegalStateException(
                    String.format(
                            "Restored committables with different transaction id prefixes: %s and %s.",
                            previousTransactionIdPrefix,
                            restoredCommittable.getTransactionalIdPrefix()));
        }
    }
}
