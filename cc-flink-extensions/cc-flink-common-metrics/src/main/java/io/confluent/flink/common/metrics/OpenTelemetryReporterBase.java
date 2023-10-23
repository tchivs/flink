/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.reporter.MetricReporter;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink {@link MetricReporter} which is made to export metrics using Open Telemetry's {@link
 * MetricExporter}.
 */
@Confluent
public class OpenTelemetryReporterBase {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryReporterBase.class);

    public static final String ARG_EXPORTER_FACTORY_CLASS = "exporter.factory.class";
    protected static final String OTEL_SERVICE_NAME = "OTEL_SERVICE_NAME";
    protected static final String OTEL_SERVICE_VERSION = "OTEL_SERVICE_VERSION";

    protected final Resource resource;
    protected MetricExporter exporter;

    @VisibleForTesting
    OpenTelemetryReporterBase() {
        resource =
                Resource.getDefault()
                        .merge(
                                Resource.create(
                                        Attributes.of(
                                                ResourceAttributes.SERVICE_NAME,
                                                System.getenv(OTEL_SERVICE_NAME))))
                        .merge(
                                Resource.create(
                                        Attributes.of(
                                                ResourceAttributes.SERVICE_VERSION,
                                                System.getenv(OTEL_SERVICE_VERSION))));
    }
}
