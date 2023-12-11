/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.compute.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.token.DelegationTokenProvider;
import org.apache.flink.util.InstantiationUtil;

import io.confluent.flink.credentials.FileCredentialDecrypterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * This class provides key tokens for a ComputePool cluster.
 *
 * <p>In more detail, {@link ComputePoolDelegationTokenProvider} is created on the JM and {@link
 * DelegationTokenProvider#obtainDelegationTokens()} is fetching the mounted ComputePool key token
 * once, serializes it, and does an rpc to each TM. The TMs all instantiate an instance of {@link
 * ComputePoolDelegationTokenReceiver} on startup, and upon getting the rpc, pass the serialized
 * token to the receiver. This allows this class to do the fetching of tokens in a single place and
 * then fans out the tokens to all the TMs.
 */
@Confluent
public class ComputePoolDelegationTokenProvider implements DelegationTokenProvider {
    private static final Logger LOG =
            LoggerFactory.getLogger(ComputePoolDelegationTokenProvider.class);
    private final FileCredentialDecrypterImpl mountedCredentials;

    /**
     * Creates a new ComputePoolDelegationTokenProvider.
     *
     * <p>It uses the default {@link FileCredentialDecrypterImpl} to decrypt the credentials.
     */
    public ComputePoolDelegationTokenProvider() {
        this.mountedCredentials = FileCredentialDecrypterImpl.INSTANCE;
    }

    @VisibleForTesting
    public ComputePoolDelegationTokenProvider(FileCredentialDecrypterImpl credentials) {
        this.mountedCredentials = credentials;
    }

    @Override
    public String serviceName() {
        return "ComputePool";
    }

    @Override
    public void init(Configuration configuration) {
        LOG.info("ComputePool token provider initialized");
    }

    @Override
    public boolean delegationTokensRequired() {
        return true;
    }

    @Override
    public ObtainedDelegationTokens obtainDelegationTokens() throws IOException {
        // ComputePool key will be valid forever
        LOG.debug("Sending ComputePool token");
        return new ObtainedDelegationTokens(
                InstantiationUtil.serializeObject(mountedCredentials.getPrivateKey()),
                Optional.empty());
    }
}
