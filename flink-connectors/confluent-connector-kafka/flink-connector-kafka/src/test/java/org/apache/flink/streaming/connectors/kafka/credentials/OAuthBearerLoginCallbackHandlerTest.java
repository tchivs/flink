/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.security.token.kafka.KafkaCredentials;
import org.apache.flink.core.security.token.kafka.util.MockKafkaCredentialsCache;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.callback.Callback;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.kafka.credentials.OAuthBearerJwsToken.OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY;
import static org.apache.flink.streaming.connectors.kafka.credentials.OAuthBearerLoginCallbackHandler.MODULE_CONFIG_JOB_ID;
import static org.apache.flink.streaming.connectors.kafka.credentials.OAuthBearerLoginCallbackHandler.MODULE_CONFIG_LOGICAL_CLUSTER_ID;
import static org.apache.flink.streaming.connectors.kafka.credentials.OAuthBearerLoginCallbackHandler.PROPERTY_KAFKA_TOKEN_EXPIRATION_MS;
import static org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link OAuthBearerLoginCallbackHandler}. */
@Confluent
public class OAuthBearerLoginCallbackHandlerTest {
    private static final JobID JOB_ID = new JobID(10, 20);
    private static final String TOKEN = "token_abc";

    private MockKafkaCredentialsCache credentialsCache;
    private OAuthBearerLoginCallbackHandler handler;
    private Map<String, Object> config = new HashMap<>();
    private Map<String, String> moduleOptions = new HashMap<>();
    private OAuthBearerTokenCallback oAuthBearerTokenCallback = new OAuthBearerTokenCallback();
    private SaslExtensionsCallback saslExtensionsCallback = new SaslExtensionsCallback();

    @Before
    public void setUp() throws IOException, NoSuchAlgorithmException {
        credentialsCache = new MockKafkaCredentialsCache();
        handler = new OAuthBearerLoginCallbackHandler(credentialsCache);
    }

    private void setToken(String token) throws Exception {
        Map<JobID, KafkaCredentials> map = new HashMap<>();
        map.put(JOB_ID, new KafkaCredentials(token));
        credentialsCache.onNewCredentialsObtained(map);
    }

    @Test
    public void testHandle() throws Exception {
        setToken(TOKEN);
        moduleOptions.put(MODULE_CONFIG_LOGICAL_CLUSTER_ID, "lkc-abc");
        moduleOptions.put(MODULE_CONFIG_JOB_ID, "000000000000000a0000000000000014");
        handler.configure(
                config,
                OAUTHBEARER_MECHANISM,
                ImmutableList.of(
                        new AppConfigurationEntry(
                                "module", LoginModuleControlFlag.REQUIRED, moduleOptions)));
        handler.handle(new Callback[] {oAuthBearerTokenCallback, saslExtensionsCallback});
        assertThat(oAuthBearerTokenCallback.token().value()).isEqualTo(TOKEN);
        assertThat(saslExtensionsCallback.extensions().map())
                .contains(entry(OAUTH_NEGOTIATED_LOGICAL_CLUSTER_PROPERTY_KEY, "lkc-abc"));
    }

    @Test
    public void testError_noCluster() throws Exception {
        setToken(TOKEN);
        moduleOptions.put(MODULE_CONFIG_JOB_ID, "000000000000000a0000000000000014");
        assertThatThrownBy(
                        () ->
                                handler.configure(
                                        config,
                                        OAUTHBEARER_MECHANISM,
                                        ImmutableList.of(
                                                new AppConfigurationEntry(
                                                        "module",
                                                        LoginModuleControlFlag.REQUIRED,
                                                        moduleOptions))))
                .hasMessage("Logical cluster id for token must be set in the JAAS config.");
    }

    @Test
    public void testError_noJobId() throws Exception {
        setToken(TOKEN);
        moduleOptions.put(MODULE_CONFIG_LOGICAL_CLUSTER_ID, "lkc-abc");
        assertThatThrownBy(
                        () ->
                                handler.configure(
                                        config,
                                        OAUTHBEARER_MECHANISM,
                                        ImmutableList.of(
                                                new AppConfigurationEntry(
                                                        "module",
                                                        LoginModuleControlFlag.REQUIRED,
                                                        moduleOptions))))
                .hasMessage("Job id for token must be set in the JAAS config.");
    }

    @Test
    public void testError_badJobId() throws Exception {
        setToken(TOKEN);
        moduleOptions.put(MODULE_CONFIG_LOGICAL_CLUSTER_ID, "lkc-abc");
        moduleOptions.put(MODULE_CONFIG_JOB_ID, "abc");
        assertThatThrownBy(
                        () ->
                                handler.configure(
                                        config,
                                        OAUTHBEARER_MECHANISM,
                                        ImmutableList.of(
                                                new AppConfigurationEntry(
                                                        "module",
                                                        LoginModuleControlFlag.REQUIRED,
                                                        moduleOptions))))
                .hasMessageContaining("Cannot parse JobID from \"abc\"");
    }

    @Test
    public void testError_badExpiration() throws Exception {
        setToken(TOKEN);
        config.put(PROPERTY_KAFKA_TOKEN_EXPIRATION_MS, "abc");
        moduleOptions.put(MODULE_CONFIG_LOGICAL_CLUSTER_ID, "lkc-abc");
        moduleOptions.put(MODULE_CONFIG_JOB_ID, "000000000000000a0000000000000014");
        assertThatThrownBy(
                        () ->
                                handler.configure(
                                        config,
                                        OAUTHBEARER_MECHANISM,
                                        ImmutableList.of(
                                                new AppConfigurationEntry(
                                                        "module",
                                                        LoginModuleControlFlag.REQUIRED,
                                                        moduleOptions))))
                .hasMessageContaining("Bad expiration time abc");
    }

    @Test
    public void testGoodExpiration() throws Exception {
        setToken(TOKEN);
        config.put(PROPERTY_KAFKA_TOKEN_EXPIRATION_MS, "5000");
        moduleOptions.put(MODULE_CONFIG_LOGICAL_CLUSTER_ID, "lkc-abc");
        moduleOptions.put(MODULE_CONFIG_JOB_ID, "000000000000000a0000000000000014");
        handler.configure(
                config,
                OAUTHBEARER_MECHANISM,
                ImmutableList.of(
                        new AppConfigurationEntry(
                                "module", LoginModuleControlFlag.REQUIRED, moduleOptions)));

        config.put(PROPERTY_KAFKA_TOKEN_EXPIRATION_MS, 5000L);
        handler.configure(
                config,
                OAUTHBEARER_MECHANISM,
                ImmutableList.of(
                        new AppConfigurationEntry(
                                "module", LoginModuleControlFlag.REQUIRED, moduleOptions)));
    }
}
