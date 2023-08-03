/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v3;

import io.confluent.flink.jobgraph.VersionedFlinkIntegration;

import java.io.IOException;

/** Version 3 of {@link VersionedFlinkIntegration} implementation. */
public class FlinkIntegrationImplV3 implements FlinkIntegrationV3 {

    @Override
    public JobGraphGeneratorV3 getJobGraphGenerator() {
        return new CompiledPlanJobGraphGeneratorV3Impl();
    }

    @Override
    public void close() throws IOException {
        // No-op.
    }
}
