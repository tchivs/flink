/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import javax.annotation.Nullable;

import java.io.Closeable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Used for aborting any lingering transactions from previous executions of the sink. */
public abstract class TransactionAborter implements Closeable {

    protected final String transactionalIdPrefix;
    protected final int subtaskIndex;
    protected final int numSubtasks;
    protected @Nullable final KafkaWriterState recoveredState;
    protected final InternalKafkaProducerFactory<?, ?, ?> producerFactory;

    TransactionAborter(
            String transactionalIdPrefix,
            int subtaskIndex,
            int numSubtasks,
            @Nullable KafkaWriterState recoveredState,
            InternalKafkaProducerFactory<?, ?, ?> producerFactory) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix);
        this.subtaskIndex = subtaskIndex;
        this.numSubtasks = numSubtasks;
        this.recoveredState = recoveredState;
        this.producerFactory = checkNotNull(producerFactory);
    }

    protected abstract void abortLingeringTransactions();

    public abstract void close();
}
