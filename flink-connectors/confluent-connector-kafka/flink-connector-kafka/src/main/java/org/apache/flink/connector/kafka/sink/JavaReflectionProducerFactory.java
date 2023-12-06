/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;

import java.util.Properties;
import java.util.function.Consumer;

/**
 * A {@link InternalKafkaProducerFactory} for the Java-reflection based {@link
 * FlinkKafkaInternalProducer}.
 */
public class JavaReflectionProducerFactory<K, V>
        extends InternalKafkaProducerFactory<K, V, FlinkKafkaInternalProducer<K, V>> {

    JavaReflectionProducerFactory(
            Properties baseKafkaClientProperties,
            Consumer<InternalKafkaProducer<K, V>> closerRegistry,
            Consumer<InternalKafkaProducer<K, V>> kafkaMetricsInitializer) {
        super(baseKafkaClientProperties, closerRegistry, kafkaMetricsInitializer);
    }

    @Override
    protected FlinkKafkaInternalProducer<K, V> createProducerInstance(
            Properties resolvedProperties) {
        @Nullable
        final String transactionalId =
                resolvedProperties.getProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        return new FlinkKafkaInternalProducer<>(resolvedProperties, transactionalId);
    }
}
