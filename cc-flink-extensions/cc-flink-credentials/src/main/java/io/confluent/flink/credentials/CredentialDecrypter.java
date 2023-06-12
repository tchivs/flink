/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;

/** A class that can decrypt static credentials returned by Flink Credential Service. */
@Confluent
public interface CredentialDecrypter {

    /**
     * Initializes the decrypter.
     *
     * @param configuration The Flink configuration
     */
    void init(Configuration configuration);

    /**
     * Decrypts data from Flink Credential Service.
     *
     * @param value The value to decrypt
     * @return The plain text value
     */
    byte[] decrypt(byte[] value);
}
