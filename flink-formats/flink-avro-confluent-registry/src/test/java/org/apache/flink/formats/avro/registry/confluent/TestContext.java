/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

/** Context for testing. */
@Confluent
public class TestContext
        implements InitializationContext, DeserializationSchema.InitializationContext {
    public static final TestContext CONTEXT = new TestContext();

    @Override
    public MetricGroup getMetricGroup() {
        return null;
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        return null;
    }
}
