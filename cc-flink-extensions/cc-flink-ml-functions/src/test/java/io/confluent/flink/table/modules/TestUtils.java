/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogTable;

import io.confluent.flink.compute.credentials.InMemoryCredentialDecrypterImpl;
import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;
import io.confluent.flink.table.modules.ml.secrets.FlinkCredentialServiceSecretDecrypter;
import io.confluent.flink.table.modules.ml.secrets.MeteredSecretDecrypter;
import io.confluent.flink.table.modules.ml.secrets.PlainTextDecrypter;
import io.confluent.flink.table.modules.ml.secrets.SecretDecrypter;
import io.confluent.flink.table.modules.ml.secrets.SecretDecrypterProvider;
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
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

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

    /** Mock for {@link FlinkCredentialServiceSecretDecrypter}. */
    public static class MockFlinkCredentialServiceDecrypter<T> implements SecretDecrypter {
        private final ModelOptionsUtils modelOptionsUtils;
        private final boolean shouldThrow;

        public MockFlinkCredentialServiceDecrypter(T model) {
            this(model, false);
        }

        public MockFlinkCredentialServiceDecrypter(T model, boolean shouldThrow) {
            if (model instanceof CatalogModel) {
                modelOptionsUtils = new ModelOptionsUtils(((CatalogModel) model).getOptions());
            } else if (model instanceof CatalogTable) {
                modelOptionsUtils = new ModelOptionsUtils(((CatalogTable) model).getOptions());
            } else {
                throw new IllegalArgumentException("Not supported " + model.getClass());
            }
            this.shouldThrow = shouldThrow;
        }

        @Override
        public String decryptFromKey(String secretKey) {
            if (shouldThrow) {
                throw new RuntimeException("some error");
            }
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

        @Override
        public String getProviderName() {
            return MLModelSupportedProviders.fromString(modelOptionsUtils.getProvider())
                    .getProviderName();
        }
    }

    /** Mock for {@link SecretDecrypterProvider}. */
    public static class MockSecretDecypterProvider<T> implements SecretDecrypterProvider {
        private final T model;
        private final MLFunctionMetrics metrics;
        private final Clock clock;

        public MockSecretDecypterProvider(T model, MLFunctionMetrics metrics, Clock clock) {
            this.model = model;
            this.metrics = metrics;
            this.clock = clock;
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

        @Override
        public MeteredSecretDecrypter getMeteredDecrypter(String decryptStrategy) {
            return new MeteredSecretDecrypter(getDecrypter(decryptStrategy), metrics, clock);
        }
    }

    /** Mock clock for testing. */
    public static class IncrementingClock extends Clock {
        private Instant instant;
        private final ZoneId zone;

        public IncrementingClock(Instant fixedInstant, ZoneId zone) {
            this.instant = fixedInstant;
            this.zone = zone;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return new IncrementingClock(instant, zone);
        }

        @Override
        public Instant instant() {
            instant = instant.plusMillis(1);
            return instant;
        }
    }

    /** Metrics Group to track metrics. */
    public static class TrackingMetricsGroup extends UnregisteredMetricsGroup {
        private final Map<String, Counter> counters;
        private final Map<String, Gauge<?>> gauges;
        private final String base;

        public TrackingMetricsGroup(
                String name, Map<String, Counter> counters, Map<String, Gauge<?>> gauges) {
            this.base = name;
            this.counters = counters;
            this.gauges = gauges;
        }

        @Override
        public Counter counter(String name) {
            Counter counter = new SimpleCounter();
            counters.put(base + "." + name, counter);
            return counter;
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            counters.put(base + "." + name, counter);
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            gauges.put(base + "." + name, gauge);
            return gauge;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return new TrackingMetricsGroup(base + "." + name, counters, gauges);
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return new TrackingMetricsGroup(base + "." + key + "." + value, counters, gauges);
        }
    }
}
