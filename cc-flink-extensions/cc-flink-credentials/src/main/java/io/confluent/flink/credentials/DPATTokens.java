/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;

import java.util.Optional;

/**
 * Tokens fetched from cc-auth-service to access CCloud resources like Kafka and Schema Registry.
 */
@Confluent
public class DPATTokens {

    private final String token;

    private final Optional<String> udfToken;

    public DPATTokens(String token) {
        this.token = token;
        this.udfToken = Optional.empty();
    }

    public DPATTokens(String token, Optional<String> udfToken) {
        this.token = token;
        this.udfToken = udfToken;
    }

    public String getToken() {
        return token;
    }

    public Optional<String> getUDFToken() {
        return udfToken;
    }
}
