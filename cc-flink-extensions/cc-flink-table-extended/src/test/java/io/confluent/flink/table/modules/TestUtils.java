/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules;

import org.apache.flink.table.catalog.CatalogModel;

import io.confluent.flink.compute.credentials.InMemoryCredentialDecrypterImpl;
import io.confluent.flink.table.modules.ml.PlainTextDecrypter;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.confluent.flink.table.modules.ml.SecretDecrypter;
import io.confluent.flink.table.modules.ml.SecretDecrypterProvider;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PSSParameterSpec;

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

    public static boolean verifySignature(
            String data, byte[] signature, java.security.PublicKey pubKey) throws Exception {
        Signature privateSignature = Signature.getInstance("SHA256withRSA/PSS");
        privateSignature.setParameter(
                new PSSParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1));
        privateSignature.initVerify(pubKey);
        privateSignature.update(data.getBytes(StandardCharsets.UTF_8));
        return privateSignature.verify(signature);
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

    /**
     * Mock for {@link io.confluent.flink.table.modules.ml.FlinkCredentialServiceSecretDecrypter}.
     */
    public static class MockFlinkCredentialServiceDecrypter implements SecretDecrypter {
        private final ModelOptionsUtils modelOptionsUtils;

        public MockFlinkCredentialServiceDecrypter(CatalogModel model) {
            modelOptionsUtils = new ModelOptionsUtils(model.getOptions());
        }

        @Override
        public String decryptFromKey(String secretKey) {
            String secret = modelOptionsUtils.getOption(secretKey);
            if (secret == null || secret.isEmpty()) {
                return "";
            }
            return "decypted-" + secret;
        }

        @Override
        public EncryptionStrategy supportedStrategy() {
            return EncryptionStrategy.KMS;
        }
    }

    /** Mock for {@link io.confluent.flink.table.modules.ml.SecretDecrypterProvider}. */
    public static class MockSecretDecypterProvider implements SecretDecrypterProvider {
        private final CatalogModel model;

        public MockSecretDecypterProvider(CatalogModel model) {
            this.model = model;
        }

        @Override
        public SecretDecrypter getDecrypter(String decryptStrategy) {
            if (decryptStrategy == null
                    || decryptStrategy.equalsIgnoreCase(EncryptionStrategy.PLAINTEXT.name())) {
                return new PlainTextDecrypter(model);
            } else if (decryptStrategy.equalsIgnoreCase(EncryptionStrategy.KMS.name())) {
                return new MockFlinkCredentialServiceDecrypter(model);
            }
            throw new IllegalArgumentException("Not supported " + decryptStrategy);
        }
    }
}
