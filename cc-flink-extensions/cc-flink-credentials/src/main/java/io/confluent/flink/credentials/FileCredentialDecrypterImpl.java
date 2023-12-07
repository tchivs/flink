/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import java.io.FileReader;
import java.security.KeyPair;
import java.security.PrivateKey;

import static io.confluent.flink.credentials.KafkaCredentialsOptions.MOUNTED_SECRET;

/**
 * Reads a secret which is locally mounted in the JM pod, and uses that to decrypt the API key from
 * the FCP Credential Service.
 */
public class FileCredentialDecrypterImpl extends AbstractCredentialDecrypterImpl {
    public static final FileCredentialDecrypterImpl INSTANCE = new FileCredentialDecrypterImpl();
    private static byte[] privateKey;

    private Configuration configuration;

    @Override
    public void init(Configuration configuration) {
        this.configuration = configuration;
        setPrivateKey();
    }

    public synchronized byte[] getPrivateKey() {
        return privateKey;
    }

    @Override
    protected byte[] readPrivateKey() {
        try {
            // Read the private key from the file
            PEMParser parser =
                    new PEMParser(new FileReader(configuration.getString(MOUNTED_SECRET)));
            // Convert from PKCS1 to PKCS8
            JcaPEMKeyConverter converter =
                    new JcaPEMKeyConverter().setProvider(new BouncyCastleProvider());
            KeyPair kp = converter.getKeyPair((PEMKeyPair) parser.readObject());
            PrivateKey privateKey = kp.getPrivate();
            return privateKey.getEncoded();
        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt message", e);
        }
    }

    private synchronized void setPrivateKey() {
        try {
            privateKey = readPrivateKey();
        } catch (Throwable e) {
            throw new FlinkRuntimeException("Couldn't read private key", e);
        }
    }
}
