/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.core.security.token.kafka;

import org.apache.flink.annotation.Confluent;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents credentials fetched for Kafka. These are represented by properties which override the
 * static ones set for the connector.
 */
@Confluent
public class KafkaCredentials implements Serializable {

    private static final long serialVersionUID = -8396558615187136090L;

    private final String dpatToken;

    // Is nullable to be Serializable
    private final @Nullable String udfDpatToken;

    public KafkaCredentials(String dpatToken) {
        this(dpatToken, Optional.empty());
    }

    public KafkaCredentials(String dpatToken, Optional<String> udfDpatToken) {
        this.dpatToken = dpatToken;
        this.udfDpatToken = udfDpatToken.orElse(null);
    }

    public String getDpatToken() {
        return dpatToken;
    }

    public Optional<String> getUdfDpatToken() {
        return Optional.ofNullable(udfDpatToken);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof KafkaCredentials)) {
            return false;
        }
        KafkaCredentials that = (KafkaCredentials) o;
        return Objects.equals(dpatToken, that.dpatToken)
                && Objects.equals(udfDpatToken, that.udfDpatToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dpatToken, udfDpatToken);
    }
}
