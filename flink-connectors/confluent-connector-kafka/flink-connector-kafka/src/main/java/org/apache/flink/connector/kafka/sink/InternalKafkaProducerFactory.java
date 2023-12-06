/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Factory for {@link InternalKafkaProducer} instances. */
public abstract class InternalKafkaProducerFactory<K, V, PT extends InternalKafkaProducer<K, V>> {

    private final Properties baseKafkaClientProperties;
    private final Consumer<InternalKafkaProducer<K, V>> closerRegistry;
    private final Consumer<InternalKafkaProducer<K, V>> kafkaMetricsInitializer;

    InternalKafkaProducerFactory(
            Properties baseKafkaClientProperties,
            Consumer<InternalKafkaProducer<K, V>> closerRegistry,
            Consumer<InternalKafkaProducer<K, V>> kafkaMetricsInitializer) {
        this.baseKafkaClientProperties = checkNotNull(baseKafkaClientProperties);
        this.closerRegistry = checkNotNull(closerRegistry);
        this.kafkaMetricsInitializer = checkNotNull(kafkaMetricsInitializer);
    }

    public PT createTransactional(String transactionalId) {
        final Properties resolvedProps = new Properties();
        resolvedProps.putAll(baseKafkaClientProperties);
        resolvedProps.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        final PT transactionalProducer = createProducerInstance(resolvedProps);
        closerRegistry.accept(transactionalProducer);
        kafkaMetricsInitializer.accept(transactionalProducer);
        return transactionalProducer;
    }

    public PT createNonTransactional() {
        final PT nonTransactionalProducer = createProducerInstance(baseKafkaClientProperties);
        closerRegistry.accept(nonTransactionalProducer);
        kafkaMetricsInitializer.accept(nonTransactionalProducer);
        return nonTransactionalProducer;
    }

    protected abstract PT createProducerInstance(Properties resolvedProperties);
}
