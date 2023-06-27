/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v2;

import io.confluent.flink.jobgraph.VersionedFlinkIntegration;

import java.io.IOException;

/** Version 2 of {@link VersionedFlinkIntegration} implementation. */
public class FlinkIntegrationImplV2 implements FlinkIntegrationV2 {

    @Override
    public JobGraphGeneratorV2 getJobGraphGenerator() {
        return new CompiledPlanJobGraphGeneratorV2Impl();
    }

    @Override
    public FlinkClientWrapperV2 getFlinkClientWrapper() {
        return new FlinkClientWrapperV2Impl();
    }

    @Override
    public void close() throws IOException {
        // No-op.
    }
}
