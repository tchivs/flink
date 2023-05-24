/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;

/** Token fetched from cc-auth-service to access CCloud resources like Kafka and Schema Registry. */
@Confluent
public class DPATToken {

    private final String token;

    public DPATToken(String token) {
        this.token = token;
    }

    public String getToken() {
        return token;
    }
}
