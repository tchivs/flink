/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.events.reporter.EventReporter;
import org.apache.flink.events.reporter.EventReporterFactory;

import java.util.Properties;

/** A factory for creating a {@link OpenTelemetryLogRecordReporter}. */
@Confluent
public class OpenTelemetryLogRecordReporterFactory implements EventReporterFactory {

    @Override
    public EventReporter createEventReporter(Properties properties) {
        return new OpenTelemetryLogRecordReporter();
    }
}
