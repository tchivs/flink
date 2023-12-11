/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.compute.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.core.security.token.DelegationTokenProvider.ObtainedDelegationTokens;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.compute.credentials.utils.MockFileCredentialDecrypterImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ComputePoolDelegationTokenProvider}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class ComputePoolDelegationTokenProviderTest {

    private MockFileCredentialDecrypterImpl credentials;

    private ComputePoolDelegationTokenProvider provider;

    @BeforeEach
    public void setUp() {
        credentials = new MockFileCredentialDecrypterImpl();
        provider = new ComputePoolDelegationTokenProvider(credentials);
    }

    private byte[] getKeyToken() throws Exception {
        assertThat(provider.delegationTokensRequired()).isEqualTo(true);
        ObtainedDelegationTokens obtainDelegationTokens = provider.obtainDelegationTokens();
        byte[] keyToken = obtainDelegationTokens.getTokens();
        return InstantiationUtil.deserializeObject(keyToken, this.getClass().getClassLoader());
    }

    @Test
    public void testObtainEmptyKey() throws Exception {
        credentials = credentials.withPrivateKeyBytes(new byte[0]);
        byte[] computePoolKey = getKeyToken();
        assertThat(computePoolKey).isEmpty();
    }

    @Test
    public void testObtainSimpleKey() throws Exception {
        credentials.withPrivateKeyBytes("hello world".getBytes(StandardCharsets.UTF_8));
        byte[] computePoolKey = getKeyToken();
        assertThat(new String(computePoolKey, StandardCharsets.UTF_8)).isEqualTo("hello world");
    }
}
