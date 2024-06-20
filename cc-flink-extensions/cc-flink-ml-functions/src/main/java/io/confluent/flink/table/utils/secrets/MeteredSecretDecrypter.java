/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.secrets;

import io.confluent.flink.table.modules.ml.MLFunctionMetrics;

import java.time.Clock;
import java.util.Objects;

/** SecretDecrypter with metrics. */
public final class MeteredSecretDecrypter {
    private final MLFunctionMetrics metrics;
    private final Clock clock;
    private final SecretDecrypter secretDecrypter;

    public MeteredSecretDecrypter(
            SecretDecrypter secretDecrypter, MLFunctionMetrics metrics, Clock clock) {
        Objects.requireNonNull(secretDecrypter, "secretDecrypter");
        Objects.requireNonNull(metrics, "metrics");
        Objects.requireNonNull(clock, "clock");
        this.secretDecrypter = secretDecrypter;
        this.metrics = metrics;
        this.clock = clock;
    }

    public String decryptFromKey(final String secretKey) {
        long startMs = clock.millis();
        try {
            String decrypted = secretDecrypter.decryptFromKey(secretKey);
            metrics.decryptRequestSuccess(
                    secretDecrypter.getProviderName(), secretDecrypter.supportedStrategy().name());
            return decrypted;
        } catch (Exception e) {
            metrics.decryptRequestFailure(
                    secretDecrypter.getProviderName(), secretDecrypter.supportedStrategy().name());
            throw e;
        } finally {
            metrics.decryptRequestMs(
                    secretDecrypter.getProviderName(),
                    secretDecrypter.supportedStrategy().name(),
                    clock.millis() - startMs);
        }
    }
}
