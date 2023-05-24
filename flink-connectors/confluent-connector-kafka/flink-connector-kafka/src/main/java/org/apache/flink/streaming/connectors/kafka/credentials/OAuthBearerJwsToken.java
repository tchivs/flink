/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;

import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

import java.util.Collections;
import java.util.Set;

/** The OAuthBearerToken used by Kafka to represent the token we've fetched. */
@Confluent
public class OAuthBearerJwsToken implements OAuthBearerToken {

    public static final String OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY = "logicalCluster";

    private final String value;
    private final String principalName;
    private final Set<String> scope;
    private final long lifetimeMs;
    private final Long startTimeMs;

    public OAuthBearerJwsToken(String value, long expirationMs) {
        this.value = value;
        this.principalName = "";
        this.scope = Collections.emptySet();
        this.lifetimeMs = System.currentTimeMillis() + expirationMs;
        this.startTimeMs = System.currentTimeMillis() - 1;
    }

    @Override
    public String value() {
        return value;
    }

    @Override
    public Set<String> scope() {
        return scope;
    }

    @Override
    public long lifetimeMs() {
        return lifetimeMs;
    }

    @Override
    public String principalName() {
        return principalName;
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs;
    }
}
