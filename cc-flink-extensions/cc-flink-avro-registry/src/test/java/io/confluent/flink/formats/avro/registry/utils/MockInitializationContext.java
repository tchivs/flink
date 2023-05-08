/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * A mock {@link
 * org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext} and {@link
 * org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext}.
 */
public class MockInitializationContext
        implements DeserializationSchema.InitializationContext,
                SerializationSchema.InitializationContext {

    @Override
    public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
    }
}
