/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.connectors.kafka.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;

import java.io.FileReader;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;

import static org.apache.flink.streaming.connectors.kafka.credentials.KafkaCredentialsOptions.MOUNTED_SECRET;

/**
 * Reads a secret which is locally mounted in the pod, and uses that to decrypt the API key from the
 * FCP Credential Service.
 */
@Confluent
public class CredentialDecrypterImpl implements CredentialDecrypter {
    private static final Logger LOG = LoggerFactory.getLogger(CredentialDecrypterImpl.class);

    public static final CredentialDecrypterImpl INSTANCE = new CredentialDecrypterImpl();

    private static byte[] privateKey;

    public void init(Configuration configuration) {
        setPrivateKey(configuration);
    }

    @Override
    public byte[] decrypt(byte[] value) {
        byte[] privateKeyBytes = getPrivateKey();
        try {
            PKCS8EncodedKeySpec ks = new PKCS8EncodedKeySpec(privateKeyBytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            PrivateKey privateKey = kf.generatePrivate(ks);

            Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPPadding");
            OAEPParameterSpec oaepParams =
                    new OAEPParameterSpec(
                            "SHA-256",
                            "MGF1",
                            new MGF1ParameterSpec("SHA-256"),
                            PSource.PSpecified.DEFAULT);
            cipher.init(Cipher.DECRYPT_MODE, privateKey, oaepParams);
            return cipher.doFinal(value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to decrypt message", e);
        }
    }

    /** Singleton, so no public constructor. */
    protected CredentialDecrypterImpl() {}

    private byte[] readPrivateKey(Configuration configuration) throws IOException {
        PEMParser parser = new PEMParser(new FileReader(configuration.getString(MOUNTED_SECRET)));
        // Convert from PKCS1 to PKCS8
        JcaPEMKeyConverter converter =
                new JcaPEMKeyConverter().setProvider(new BouncyCastleProvider());
        KeyPair kp = converter.getKeyPair((PEMKeyPair) parser.readObject());
        PrivateKey privateKey = kp.getPrivate();
        return privateKey.getEncoded();
    }

    private synchronized void setPrivateKey(Configuration configuration) {
        try {
            privateKey = readPrivateKey(configuration);
        } catch (Throwable e) {
            throw new FlinkRuntimeException("Couldn't read private key", e);
        }
    }

    private synchronized byte[] getPrivateKey() {
        return privateKey;
    }
}
