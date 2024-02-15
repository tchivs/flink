/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Confluent;

import java.util.Optional;

/**
 * Interface for snapshotted committables. Implementations can contain version specific information
 * beyond the transactional.id.
 *
 * <p>- V1 / {@link KafkaCommittableV1}: additionally contains producer ID and epoch obtained via
 * Java reflection on the producer client.
 */
@Confluent
public interface KafkaCommittable {

    int getVersion();

    String getTransactionalId();

    Optional<Recyclable<? extends InternalKafkaProducer<?, ?>>> getProducer();
}
