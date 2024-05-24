/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.table.modules.ml.RemoteModelOptions.EncryptionStrategy;
import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** All the metrics for ML functions. */
public class MLFunctionMetrics {
    static final String METRIC_NAME = "ConfluentML";

    // For Azure AI Studio endpoints in AzureML.
    public static final String AZURE_ML_AI = "AZUREML-AI";
    // For Publisher models in GCP Vertex AI.
    public static final String VERTEX_PUB = "VERTEX-PUB";

    static final String PROVISIONS_NAME = "provisions";
    static final String DEPROVISIONS_NAME = "deprovisions";
    static final String REQUESTS = "requests";
    static final String REQUEST_SUCCESSES = "requestSuccesses";
    static final String REQUEST_FAILURES = "requestFailures";
    static final String REQUEST_4XX = "request4XX";
    static final String REQUEST_5XX = "request5XX";
    static final String REQUEST_OTHER_HTTP = "requestOtherHttpFailures";
    static final String REQUEST_PARSE_FAIL = "parseFailures";
    static final String REQUEST_PREP_FAIL = "prepFailures";
    static final String REQUEST_MS = "requestMs";
    static final String TOTAL_MS = "totalMs";
    static final String BYES_SENT = "bytesSent";
    static final String BYES_RECEIVED = "bytesReceived";

    private final MetricGroup group;
    private final ProviderMetrics totalMetrics;
    private final Map<String, ProviderMetrics> providerMetrics;

    /** Class to hold counters for each decrypter group. */
    private static class DecrypterMetrics {
        private final Counter requestSuccess;
        private final Counter requestFailure;
        private final Gauge<Long> requestMs;
        private final AtomicLong lastRequestMs = new AtomicLong(0);

        public DecrypterMetrics(MetricGroup group) {
            this.requestSuccess = group.counter(REQUEST_SUCCESSES);
            this.requestFailure = group.counter(REQUEST_FAILURES);
            this.requestMs = group.gauge(REQUEST_MS, lastRequestMs::get);
        }

        public void requestMs(long requestMs) {
            lastRequestMs.set(requestMs);
        }

        public void requestSuccess() {
            requestSuccess.inc();
        }

        public void requestFailure() {
            requestFailure.inc();
        }
    }

    /** Class to hold counters for each provider group. */
    private static class ProviderMetrics {
        private final MetricGroup group;
        private final Counter provisions;
        private final Counter deprovisions;
        private final Counter requests;
        private final Counter requestSuccesses;
        private final Counter request4XX;
        private final Counter request5XX;
        private final Counter requestOtherHttpFailures;
        private final Counter requestParseFailures;
        private final Counter requestPrepFailures;
        private final Gauge<Long> requestMs;
        private final Gauge<Long> totalMs;
        private final Counter bytesSent;
        private final Counter bytesReceived;
        private final Map<String, DecrypterMetrics> decrypterMetrics;

        private final AtomicLong lastRequestMs = new AtomicLong(0);
        private final AtomicLong lastTotalMs = new AtomicLong(0);

        public ProviderMetrics(MetricGroup parentGroup) {
            this.group = parentGroup;
            this.provisions = group.counter(PROVISIONS_NAME);
            this.deprovisions = group.counter(DEPROVISIONS_NAME);
            this.requests = group.counter(REQUESTS);
            this.requestSuccesses = group.counter(REQUEST_SUCCESSES);
            this.request4XX = group.counter(REQUEST_4XX);
            this.request5XX = group.counter(REQUEST_5XX);
            this.requestOtherHttpFailures = group.counter(REQUEST_OTHER_HTTP);
            this.requestParseFailures = group.counter(REQUEST_PARSE_FAIL);
            this.requestPrepFailures = group.counter(REQUEST_PREP_FAIL);
            this.requestMs = group.gauge(REQUEST_MS, lastRequestMs::get);
            this.totalMs = group.gauge(TOTAL_MS, lastTotalMs::get);
            this.bytesSent = group.counter(BYES_SENT);
            this.bytesReceived = group.counter(BYES_RECEIVED);
            this.decrypterMetrics =
                    ImmutableMap.of(
                            EncryptionStrategy.KMS.name(),
                            new DecrypterMetrics(group.addGroup(EncryptionStrategy.KMS.name())),
                            EncryptionStrategy.PLAINTEXT.name(),
                            new DecrypterMetrics(
                                    group.addGroup(EncryptionStrategy.PLAINTEXT.name())));
        }

        public void provision() {
            provisions.inc();
        }

        public void deprovision() {
            deprovisions.inc();
        }

        public void request() {
            requests.inc();
        }

        public void requestMs(long requestMs) {
            lastRequestMs.set(requestMs);
        }

        public void totalMs(long totalMs) {
            lastTotalMs.set(totalMs);
        }

        public void requestSuccess() {
            requestSuccesses.inc();
        }

        public void request4XX() {
            request4XX.inc();
        }

        public void request5XX() {
            request5XX.inc();
        }

        public void requestOtherHttpFailure() {
            requestOtherHttpFailures.inc();
        }

        public void requestParseFailure() {
            requestParseFailures.inc();
        }

        public void requestPrepFailure() {
            requestPrepFailures.inc();
        }

        public void bytesSent(long numBytes) {
            bytesSent.inc(numBytes);
        }

        public void bytesReceived(long numBytes) {
            bytesReceived.inc(numBytes);
        }

        public void decryptRequestSuccess(String strategy) {
            decrypterMetrics.get(strategy).requestSuccess();
        }

        public void decryptRequestFailure(String strategy) {
            decrypterMetrics.get(strategy).requestFailure();
        }

        public void decryptRequestMs(String strategy, long ms) {
            decrypterMetrics.get(strategy).requestMs(ms);
        }
    }

    public MLFunctionMetrics(MetricGroup parentGroup) {
        this.group = parentGroup.addGroup(METRIC_NAME);
        this.totalMetrics = new ProviderMetrics(group);
        // One group per MLModelSupportedProviders
        providerMetrics = new HashMap<>();
        // Add a group for each provider
        for (MLModelSupportedProviders provider : MLModelSupportedProviders.values()) {
            providerMetrics.put(
                    provider.getProviderName(),
                    new ProviderMetrics(
                            parentGroup.addGroup(METRIC_NAME, provider.getProviderName())));
        }
        // Add extra groups for some providers to distinguish between different types of models.
        providerMetrics.put(
                AZURE_ML_AI, new ProviderMetrics(parentGroup.addGroup(METRIC_NAME, AZURE_ML_AI)));
        providerMetrics.put(
                VERTEX_PUB, new ProviderMetrics(parentGroup.addGroup(METRIC_NAME, VERTEX_PUB)));
    }

    public void provision(String provider) {
        totalMetrics.provision();
        providerMetrics.get(provider).provision();
    }

    public void deprovision(String provider) {
        totalMetrics.deprovision();
        providerMetrics.get(provider).deprovision();
    }

    public void request(String provider) {
        totalMetrics.request();
        providerMetrics.get(provider).request();
    }

    public void requestMs(String provider, long requestMs) {
        totalMetrics.requestMs(requestMs);
        providerMetrics.get(provider).requestMs(requestMs);
    }

    public void totalMs(String provider, long totalMs) {
        totalMetrics.totalMs(totalMs);
        providerMetrics.get(provider).totalMs(totalMs);
    }

    public void requestSuccess(String provider) {
        totalMetrics.requestSuccess();
        providerMetrics.get(provider).requestSuccess();
    }

    public void requestHttpFailure(String provider, int code) {
        if (code >= 400 && code < 500) {
            request4XX(provider);
        } else if (code >= 500) {
            request5XX(provider);
        } else {
            requestOtherHttpFailure(provider);
        }
    }

    public void request4XX(String provider) {
        totalMetrics.request4XX();
        providerMetrics.get(provider).request4XX();
    }

    public void request5XX(String provider) {
        totalMetrics.request5XX();
        providerMetrics.get(provider).request5XX();
    }

    public void requestOtherHttpFailure(String provider) {
        totalMetrics.requestOtherHttpFailure();
        providerMetrics.get(provider).requestOtherHttpFailure();
    }

    public void requestParseFailure(String provider) {
        totalMetrics.requestParseFailure();
        providerMetrics.get(provider).requestParseFailure();
    }

    public void requestPrepFailure(String provider) {
        totalMetrics.requestPrepFailure();
        providerMetrics.get(provider).requestPrepFailure();
    }

    public void bytesSent(String provider, long numBytes) {
        totalMetrics.bytesSent(numBytes);
        providerMetrics.get(provider).bytesSent(numBytes);
    }

    public void bytesReceived(String provider, long numBytes) {
        totalMetrics.bytesReceived(numBytes);
        providerMetrics.get(provider).bytesReceived(numBytes);
    }

    public void decryptRequestSuccess(String provider, String strategy) {
        totalMetrics.decryptRequestSuccess(strategy);
        providerMetrics.get(provider).decryptRequestSuccess(strategy);
    }

    public void decryptRequestFailure(String provider, String strategy) {
        totalMetrics.decryptRequestFailure(strategy);
        providerMetrics.get(provider).decryptRequestFailure(strategy);
    }

    public void decryptRequestMs(String provider, String strategy, long ms) {
        totalMetrics.decryptRequestMs(strategy, ms);
        providerMetrics.get(provider).decryptRequestMs(strategy, ms);
    }
}
