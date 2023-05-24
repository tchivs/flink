/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCache;
import org.apache.flink.core.security.token.kafka.KafkaCredentialsCacheImpl;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SaslExtensions;
import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.streaming.connectors.kafka.credentials.OAuthBearerJwsToken.OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY;

/** */
@Confluent
public class OAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler {
    static final String MODULE_CONFIG_LOGICAL_CLUSTER_ID = "logicalClusterId";
    static final String MODULE_CONFIG_JOB_ID = "jobId";
    static final String PROPERTY_KAFKA_TOKEN_EXPIRATION_MS = "confluent.kafka.token.expiration.ms";
    static final long DEFAULT_KAFKA_TOKEN_EXPIRATION_MS = Duration.ofMinutes(1).toMillis();

    private static final Logger log =
            LoggerFactory.getLogger(OAuthBearerLoginCallbackHandler.class);

    private final KafkaCredentialsCache credentialsCache;

    private String logicalClusterId;
    private JobID jobID;
    private long tokenExpirationMs;
    private boolean configured = false;

    public OAuthBearerLoginCallbackHandler() {
        credentialsCache = KafkaCredentialsCacheImpl.INSTANCE;
    }

    @VisibleForTesting
    OAuthBearerLoginCallbackHandler(KafkaCredentialsCache credentialsCache) {
        this.credentialsCache = credentialsCache;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(
            Map<String, ?> configs,
            String saslMechanism,
            List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException(
                    String.format("Unexpected SASL mechanism: %s", saslMechanism));
        }
        if (Objects.requireNonNull(jaasConfigEntries).size() != 1
                || jaasConfigEntries.get(0) == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                            jaasConfigEntries.size()));
        }

        Map<String, String> moduleOptions =
                Collections.unmodifiableMap(
                        (Map<String, String>) jaasConfigEntries.get(0).getOptions());
        logicalClusterId = moduleOptions.get(MODULE_CONFIG_LOGICAL_CLUSTER_ID);
        if (logicalClusterId == null || logicalClusterId.isEmpty()) {
            throw new ConfigException(
                    "Logical cluster id for token must be set in the JAAS config.");
        }
        String jobIdHexString = moduleOptions.get(MODULE_CONFIG_JOB_ID);
        if (jobIdHexString == null || jobIdHexString.isEmpty()) {
            throw new ConfigException("Job id for token must be set in the JAAS config.");
        }
        jobID = JobID.fromHexString(jobIdHexString);
        try {
            tokenExpirationMs =
                    Optional.ofNullable(configs.get(PROPERTY_KAFKA_TOKEN_EXPIRATION_MS))
                            .map(o -> o instanceof Long ? (Long) o : Long.parseLong((String) o))
                            .orElse(DEFAULT_KAFKA_TOKEN_EXPIRATION_MS);
        } catch (NumberFormatException e) {
            throw new ConfigException(
                    "Bad expiration time " + configs.get(PROPERTY_KAFKA_TOKEN_EXPIRATION_MS));
        }

        configured = true;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (!configured) {
            throw new IllegalStateException("Callback handler not configured");
        }

        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                attachAuthToken((OAuthBearerTokenCallback) callback);
            } else if (callback instanceof SaslExtensionsCallback) {
                attachTenantLogicalCluster((SaslExtensionsCallback) callback);
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    @Override
    public void close() {
        // empty
    }

    /*
       Attaches custom SASL extensions to the callback
    */
    private void attachTenantLogicalCluster(SaslExtensionsCallback callback)
            throws ConfigException {
        Map<String, String> extensions = new HashMap<>();
        extensions.put(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY, logicalClusterId);
        callback.extensions(new SaslExtensions(extensions));
    }

    private void attachAuthToken(OAuthBearerTokenCallback callback) {
        if (callback.token() != null) {
            throw new IllegalArgumentException("Callback had a token already");
        }

        Optional<KafkaCredentials> credentials = credentialsCache.getCredentials(jobID);
        // token is passed in through JAAS (not built in Kafka),
        // therefore these constructor options are ignored
        callback.token(
                new OAuthBearerJwsToken(
                        credentials.map(KafkaCredentials::getDpatToken).orElse(null),
                        tokenExpirationMs));
    }
}
