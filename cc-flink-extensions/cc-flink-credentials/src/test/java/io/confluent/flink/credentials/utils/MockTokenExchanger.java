/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials.utils;

import org.apache.flink.annotation.Confluent;

import io.confluent.flink.credentials.DPATTokens;
import io.confluent.flink.credentials.JobCredentialsMetadata;
import io.confluent.flink.credentials.TokenExchanger;
import org.apache.commons.lang3.tuple.Pair;

/** Mocks a {@link TokenExchanger}. */
@Confluent
public class MockTokenExchanger implements TokenExchanger {

    private DPATTokens token;
    private boolean error;

    public MockTokenExchanger withToken(DPATTokens token) {
        this.token = token;
        return this;
    }

    public MockTokenExchanger withError() {
        this.error = true;
        return this;
    }

    @Override
    public DPATTokens fetch(
            Pair<String, String> staticCredentials, JobCredentialsMetadata jobCredentialsMetadata) {
        if (error) {
            throw new RuntimeException("Exchange Error");
        }
        return token;
    }
}
