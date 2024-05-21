/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.compute.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.compute.credentials.utils.MockComputePoolKeyCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ComputePoolDelegationTokenReceiver}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class ComputePoolDelegationTokenReceiverTest {

    private byte[] computePoolKey1;
    private byte[] computePoolKey2;
    private MockComputePoolKeyCache computePoolKeyCache;
    private ComputePoolDelegationTokenReceiver receiver;

    @BeforeEach
    public void setUp() {
        computePoolKeyCache = new MockComputePoolKeyCache();
        receiver = new ComputePoolDelegationTokenReceiver(computePoolKeyCache);
        computePoolKey1 = "computeKey1".getBytes(StandardCharsets.UTF_8);
        computePoolKey2 = "computeKey2".getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void testReceive() throws Exception {
        assertThat(computePoolKeyCache.getPrivateKey()).isEqualTo(Optional.empty());

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(computePoolKey1));
        assertThat(computePoolKeyCache.getPrivateKey()).contains(computePoolKey1);

        receiver.onNewTokensObtained(InstantiationUtil.serializeObject(computePoolKey2));
        assertThat(computePoolKeyCache.getPrivateKey()).contains(computePoolKey2);
    }
}
