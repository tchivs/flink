/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.core.security.token.DelegationTokenReceiver;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class receives key tokens within a ComputePool cluster.
 *
 * <p>In more detail, {@link ComputePoolDelegationTokenProvider} is created on the JM and {@link
 * DelegationTokenProvider#obtainDelegationTokens()} is fetching the mounted ComputePool key token
 * once, serializes it, and does an rpc to each TM. The TMs all instantiate an instance of {@link
 * ComputePoolDelegationTokenReceiver} on startup, and upon getting the rpc, pass the serialized
 * token to the receiver. This allows this class to do the fetching of tokens in a single place and
 * then fans out the tokens to all the TMs.
 */
@Confluent
public class ComputePoolDelegationTokenReceiver implements DelegationTokenReceiver {
    private static final Logger LOG =
            LoggerFactory.getLogger(ComputePoolDelegationTokenReceiver.class);

    private final ComputePoolKeyCache computePoolkeyCache;

    /**
     * Creates a new ComputePoolDelegationTokenReceiver.
     *
     * <p>It uses the default {@link ComputePoolKeyCacheImpl} to cache the key token.
     */
    public ComputePoolDelegationTokenReceiver() {
        this.computePoolkeyCache = ComputePoolKeyCacheImpl.INSTANCE;
    }

    @VisibleForTesting
    public ComputePoolDelegationTokenReceiver(ComputePoolKeyCache computePoolkeyCache) {
        this.computePoolkeyCache = computePoolkeyCache;
    }

    @Override
    public String serviceName() {
        return "ComputePool";
    }

    @Override
    public synchronized void init(Configuration configuration) {
        LOG.info("ComputePool token receiver initialized");
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        LOG.debug("Received ComputePool token");
        byte[] computePoolKey =
                InstantiationUtil.deserializeObject(
                        tokens, ComputePoolDelegationTokenReceiver.class.getClassLoader());
        computePoolkeyCache.onNewPrivateKeyObtained(computePoolKey);
    }
}
