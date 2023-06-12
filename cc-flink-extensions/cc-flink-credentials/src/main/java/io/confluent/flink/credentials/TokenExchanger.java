/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;

import org.apache.commons.lang3.tuple.Pair;

/** Does a token exchange for a DPAT token. */
@Confluent
public interface TokenExchanger {

    /**
     * Exchanges the given static credentials for a token.
     *
     * @param staticCredentials The key and secret of the credentials.
     * @param jobCredentialsMetadata The job credentials metadata
     * @return The token
     */
    DPATToken fetch(
            Pair<String, String> staticCredentials, JobCredentialsMetadata jobCredentialsMetadata);
}
