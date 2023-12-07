/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.MGF1ParameterSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link InMemoryCredentialDecrypterImpl}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class InMemoryCredentialDecrypterImplTest {

    private KeyPair kp;
    private Configuration configuration;
    private InMemoryCredentialDecrypterImpl decrypter;
    private ComputePoolKeyCache computePoolKeyCache;

    @BeforeEach
    public void setUp() throws NoSuchAlgorithmException {

        configuration = new Configuration();
        configuration.setLong(ComputePoolKeyCacheOptions.RECEIVE_TIMEOUT_MS, 50);
        decrypter = InMemoryCredentialDecrypterImpl.INSTANCE;
        computePoolKeyCache = ComputePoolKeyCacheImpl.INSTANCE;
        kp = createKeyPair();
        writeSecret(kp);
    }

    private static KeyPair createKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.generateKeyPair();
    }

    private void writeSecret(KeyPair kp) {
        computePoolKeyCache.onNewPrivateKeyObtained(kp.getPrivate().getEncoded());
    }

    private byte[] encryptMessage(String message) throws Exception {
        Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPPadding");
        OAEPParameterSpec oaepParams =
                new OAEPParameterSpec(
                        "SHA-256",
                        "MGF1",
                        new MGF1ParameterSpec("SHA-256"),
                        PSource.PSpecified.DEFAULT);
        cipher.init(Cipher.ENCRYPT_MODE, kp.getPublic(), oaepParams);
        return cipher.doFinal(message.getBytes(StandardCharsets.UTF_8));
    }

    private String decryptMessage(byte[] bytes) {
        return new String(decrypter.decrypt(bytes), StandardCharsets.UTF_8);
    }

    @Test
    public void testDecrypt() throws Exception {
        decrypter.init(configuration);
        assertThat(decryptMessage(encryptMessage("hello"))).isEqualTo("hello");

        assertThat(decryptMessage(encryptMessage("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")))
                .isEqualTo("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    @Test
    public void testNoSecret() {
        computePoolKeyCache.onNewPrivateKeyObtained(null);
        computePoolKeyCache.init(configuration);
        assertThatThrownBy(() -> decrypter.readPrivateKey())
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Timed out waiting for compute pool key");
    }
}
