/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.streaming.connectors.kafka.credentials.DPATToken;
import org.apache.flink.streaming.connectors.kafka.credentials.JobCredentialsMetadata;
import org.apache.flink.streaming.connectors.kafka.credentials.TokenExchanger;

import org.apache.commons.lang3.tuple.Pair;

/** Mocks a {@link TokenExchanger}. */
@Confluent
public class MockTokenExchanger implements TokenExchanger {

    private DPATToken token;
    private boolean error;

    public MockTokenExchanger withToken(DPATToken token) {
        this.token = token;
        return this;
    }

    public MockTokenExchanger withError() {
        this.error = true;
        return this;
    }

    @Override
    public DPATToken fetch(
            Pair<String, String> staticCredentials, JobCredentialsMetadata jobCredentialsMetadata) {
        if (error) {
            throw new RuntimeException("Exchange Error");
        }
        return token;
    }
}
