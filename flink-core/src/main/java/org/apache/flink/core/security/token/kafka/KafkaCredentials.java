/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token.kafka;

import org.apache.flink.annotation.Confluent;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents credentials fetched for Kafka. These are represented by properties which override the
 * static ones set for the connector.
 */
@Confluent
public class KafkaCredentials implements Serializable {

    private static final long serialVersionUID = -8396558615187136090L;

    private final String dpatToken;

    public KafkaCredentials(String dpatToken) {
        this.dpatToken = dpatToken;
    }

    public String getDpatToken() {
        return dpatToken;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KafkaCredentials)) {
            return false;
        }
        KafkaCredentials that = (KafkaCredentials) o;
        return Objects.equals(dpatToken, that.dpatToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dpatToken);
    }
}
