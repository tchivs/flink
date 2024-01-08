/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.credentials.CredentialDecrypter;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/** Class implementing aiSecret function. */
public class AISecret extends ScalarFunction {

    public static final String NAME = "SECRET";
    public static final String ERROR_MESSAGE = "SECRET is null. Please SET '%s' and resubmit job.";

    private final Map<String, String> userSecretsConfig;
    private final CredentialDecrypter decrypter;

    public AISecret(CredentialDecrypter decrypter, Map<String, String> sqlSecretsConfig) {
        this.decrypter = decrypter;
        this.userSecretsConfig = sqlSecretsConfig;
    }

    public String eval(String secretName) {
        final String secretValue = userSecretsConfig.get(secretName);
        // Make sure we have a valid key
        validateSecret(secretName, secretValue);

        // First Decode and then Decrypt the Secret
        final byte[] decodedUserSecret = Base64.getDecoder().decode(secretValue);
        if (decodedUserSecret.length != 0) {
            return new String(decrypter.decrypt(decodedUserSecret), StandardCharsets.UTF_8);
        }

        throw new FlinkRuntimeException(String.format(ERROR_MESSAGE, secretName));
    }

    public static String eval() {
        String key = System.getenv("OPENAI_API_KEY");
        if (key == null) {
            throw new FlinkRuntimeException("Must set environment variable OPENAI_API_KEY");
        }
        return key;
    }

    private static void validateSecret(String name, String encryptedSecret) {
        if (encryptedSecret == null) {
            throw new FlinkRuntimeException(String.format(ERROR_MESSAGE, name));
        }
    }
}
