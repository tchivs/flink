/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;

/** Reads a secret injected from somewhere and decrypts the message using RSA. */
@Confluent
public abstract class AbstractCredentialDecrypterImpl implements CredentialDecrypter {

    protected abstract byte[] readPrivateKey();

    @Override
    public byte[] decrypt(byte[] value) {
        byte[] privateKeyBytes = readPrivateKey();
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
}
