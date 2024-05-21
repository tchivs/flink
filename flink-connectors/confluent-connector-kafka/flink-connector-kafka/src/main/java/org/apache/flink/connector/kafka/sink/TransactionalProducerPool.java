/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;

/** Pool of transactional {@link InternalKafkaProducer} to be used for checkpoints. */
@Confluent
public interface TransactionalProducerPool<K, V> {
    InternalKafkaProducer<K, V> getForCheckpoint(long checkpointId);

    int numProducers();

    void recycle(InternalKafkaProducer<K, V> committedProducer);

    void clearAll();
}
