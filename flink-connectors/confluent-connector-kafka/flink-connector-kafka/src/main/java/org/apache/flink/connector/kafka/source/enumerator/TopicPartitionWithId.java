/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.PublicEvolving;

import org.apache.kafka.common.TopicPartition;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/** A wrapper for {@link TopicPartition} as well as the internal Kafka topic ID. */
@Confluent
@PublicEvolving
public class TopicPartitionWithId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TopicPartition topicPartition;
    @Nullable private final UUID topicId;

    public TopicPartitionWithId(String topic, int partitionId, UUID topicId) {
        this(new TopicPartition(topic, partitionId), topicId);
    }

    public TopicPartitionWithId(TopicPartition topicPartition, UUID topicId) {
        this.topicPartition = topicPartition;
        this.topicId = topicId;
    }

    public String getTopicName() {
        return topicPartition.topic();
    }

    public int getPartition() {
        return topicPartition.partition();
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    @Nullable
    public UUID getTopicId() {
        return topicId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicPartitionWithId that = (TopicPartitionWithId) o;
        return topicPartition.equals(that.topicPartition) && Objects.equals(topicId, that.topicId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition, topicId);
    }

    @Override
    public String toString() {
        return "TopicPartitionWithId{"
                + "topicPartition="
                + topicPartition
                + ", topicId="
                + topicId
                + '}';
    }
}
