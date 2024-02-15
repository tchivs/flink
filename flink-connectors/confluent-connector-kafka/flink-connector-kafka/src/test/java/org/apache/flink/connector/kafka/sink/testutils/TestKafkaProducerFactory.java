/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink.testutils;

import org.apache.flink.connector.kafka.sink.ConfluentKafkaCommittableV1;
import org.apache.flink.connector.kafka.sink.InternalKafkaProducerFactory;

import org.jetbrains.annotations.Nullable;

import java.util.Properties;
import java.util.Set;

/** Mock implementation of {@link InternalKafkaProducerFactory}. */
public class TestKafkaProducerFactory
        extends InternalKafkaProducerFactory<String, String, TestKafkaProducer> {

    private final Set<String> usedTransactionIds;
    private final Set<String> abortedTransactionIds;

    public TestKafkaProducerFactory(
            Set<String> usedTransactionIds, Set<String> abortedTransactionIds) {
        super(new Properties(), ignored -> {}, ignored -> {});
        this.usedTransactionIds = usedTransactionIds;
        this.abortedTransactionIds = abortedTransactionIds;
    }

    @Override
    protected TestKafkaProducer createProducerInstance(
            Properties resolvedProperties, @Nullable ConfluentKafkaCommittableV1 committable) {
        return new TestKafkaProducer(committable, usedTransactionIds, abortedTransactionIds);
    }
}
