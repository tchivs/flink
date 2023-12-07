/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLoggerExtension;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.spec.MGF1ParameterSpec;

import static io.confluent.flink.credentials.KafkaCredentialsOptions.MOUNTED_SECRET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileCredentialDecrypterImpl}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class FileCredentialDecrypterImplTest {

    private KeyPair kp;
    private Configuration configuration;
    private FileCredentialDecrypterImpl decrypter;

    @BeforeEach
    public void setUp(@TempDir File temporaryFolder) throws IOException, NoSuchAlgorithmException {
        kp = createKeyPair();
        String secretPath = temporaryFolder.getAbsolutePath() + File.separator + "secret.key";
        writeSecret(kp, secretPath);
        configuration = new Configuration();
        configuration.setString(MOUNTED_SECRET, secretPath);
        decrypter = FileCredentialDecrypterImpl.INSTANCE;
    }

    private static KeyPair createKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return kpg.generateKeyPair();
    }

    private static void writeSecret(KeyPair kp, String path) throws IOException {
        FileWriter out = new FileWriter(path);
        JcaPEMWriter writer = new JcaPEMWriter(out);

        writer.writeObject(kp.getPrivate());
        writer.close();
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
        configuration.setString(MOUNTED_SECRET, "/tmp/34873847384783748374");

        assertThatThrownBy(() -> decrypter.init(configuration))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Couldn't read private key");
    }
}
