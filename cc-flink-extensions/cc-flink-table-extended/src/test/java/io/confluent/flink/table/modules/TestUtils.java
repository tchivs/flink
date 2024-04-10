/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules;

import io.confluent.flink.compute.credentials.InMemoryCredentialDecrypterImpl;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.MGF1ParameterSpec;

/** Test utilities for encryption and decryption. */
public class TestUtils {
    public static KeyPair createKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.generateKeyPair();
    }

    public static byte[] encryptMessage(String message, java.security.PublicKey pubKey)
            throws Exception {
        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPPadding");
        OAEPParameterSpec oaepParams =
                new OAEPParameterSpec(
                        "SHA-256",
                        "MGF1",
                        new MGF1ParameterSpec("SHA-256"),
                        PSource.PSpecified.DEFAULT);
        cipher.init(Cipher.ENCRYPT_MODE, pubKey, oaepParams);
        return cipher.doFinal(message.getBytes(StandardCharsets.UTF_8));
    }

    /** Mocked CredentialDecrypter. */
    public static class MockedInMemoryCredentialDecrypterImpl
            extends InMemoryCredentialDecrypterImpl {
        final byte[] privateKey;

        public MockedInMemoryCredentialDecrypterImpl(byte[] privateKey) {
            this.privateKey = privateKey;
        }

        @Override
        protected byte[] readPrivateKey() {
            return privateKey;
        }
    }
}
