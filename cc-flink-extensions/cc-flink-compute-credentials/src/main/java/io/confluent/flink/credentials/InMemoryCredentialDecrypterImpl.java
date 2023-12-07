/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Optional;

/**
 * Reads a secret which is cached in memory on the TM from the JM, and uses that to decrypt the API
 * key from the FCP Credential Service.
 */
@Confluent
public class InMemoryCredentialDecrypterImpl extends AbstractCredentialDecrypterImpl {

    public static final InMemoryCredentialDecrypterImpl INSTANCE =
            new InMemoryCredentialDecrypterImpl();

    @Override
    public void init(Configuration configuration) {
        // Nothing to do
    }

    @Override
    protected byte[] readPrivateKey() {
        // Get the private key from memory
        Optional<byte[]> computePoolKey = ComputePoolKeyCacheImpl.INSTANCE.getPrivateKey();
        if (!computePoolKey.isPresent()) {
            throw new FlinkRuntimeException("Couldn't read private key");
        }
        return computePoolKey.get();
    }
}
