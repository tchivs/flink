/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.metrics.MetricConfig;

import io.opentelemetry.sdk.metrics.export.MetricExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.confluent.flink.common.metrics.OtlpGrpcMetricExporterFactory.ARG_EXPORTER_ENDPOINT;
import static io.confluent.flink.common.metrics.OtlpGrpcMetricExporterFactory.ARG_EXPORTER_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** Tests for {@link OtlpGrpcMetricExporterFactory}. */
public class OtlpGrpcMetricExporterFactoryTest {

    private OtlpGrpcMetricExporterFactory factory;

    @BeforeEach
    public void setUp() {
        factory = new OtlpGrpcMetricExporterFactory();
    }

    @Test
    public void testNoEndpoint() {
        MetricConfig metricConfig = new MetricConfig();

        IllegalArgumentException exception =
                catchThrowableOfType(
                        () -> factory.createMetricExporter(metricConfig),
                        IllegalArgumentException.class);
        assertThat(exception).hasMessageContaining("Must set exporter.endpoint");
    }

    @Test
    public void testBadDuration() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_ENDPOINT, "http://example.com");
        metricConfig.setProperty(ARG_EXPORTER_TIMEOUT, "10q");

        IllegalArgumentException exception =
                catchThrowableOfType(
                        () -> factory.createMetricExporter(metricConfig),
                        IllegalArgumentException.class);
        assertThat(exception).hasMessageContaining("Time interval unit label 'q' does not match");
    }

    @Test
    public void testSuccess() {
        MetricConfig metricConfig = new MetricConfig();
        metricConfig.setProperty(ARG_EXPORTER_ENDPOINT, "http://localhost");
        metricConfig.setProperty(ARG_EXPORTER_TIMEOUT, "10s");

        MetricExporter exporter = factory.createMetricExporter(metricConfig);
        assertThat(exporter).isNotNull();
    }
}
