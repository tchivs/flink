/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;

import java.util.Properties;
import java.util.function.Consumer;

/** A {@link InternalKafkaProducerFactory} for the new {@link TwoPhaseCommitProducer}. */
@Confluent
@Internal
public class TwoPhaseCommitProducerFactory<K, V>
        extends InternalKafkaProducerFactory<K, V, TwoPhaseCommitProducer<K, V>> {

    TwoPhaseCommitProducerFactory(
            Properties baseKafkaClientProperties,
            Consumer<InternalKafkaProducer<K, V>> closerRegistry,
            Consumer<InternalKafkaProducer<K, V>> kafkaMetricsInitializer) {
        super(baseKafkaClientProperties, closerRegistry, kafkaMetricsInitializer);
    }

    @Override
    protected TwoPhaseCommitProducer<K, V> createProducerInstance(
            Properties resolvedProperties, ConfluentKafkaCommittableV1 committable) {
        return new TwoPhaseCommitProducer<>(resolvedProperties, committable);
    }
}
