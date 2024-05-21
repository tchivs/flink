/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.PublicEvolving;

import java.util.Collection;
import java.util.Set;

/**
 * A marker interface that allows a {@link Committer} to apply additional logic or bookkeeping for
 * restored committables by subtask.
 */
@Confluent
@PublicEvolving
public interface RestoredSubtaskCommittablesTracker<CommT> extends Committer<CommT> {

    /**
     * This method is called before {@link #commit(Collection)} is called to re-commit restored
     * committables.
     *
     * @param restoredSubtaskIds ids of subtasks' which has their committables restored to this
     *     committer.
     */
    void preCommitRestoredSubtaskCommittables(Set<Integer> restoredSubtaskIds);

    /**
     * This method is called after all restored committables have been attempted to be committed via
     * {@link #commit(Collection)}.
     */
    void postCommitRestoredSubtaskCommittables();
}
