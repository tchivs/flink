/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

/** Default {@link FlinkIntegration} implementation. */
public class FlinkIntegrationImpl implements FlinkIntegration {
    @Override
    public JobGraphGenerator getJobGraphGenerator() {
        return new CompiledPlanJobGraphGeneratorImpl();
    }

    @Override
    public FlinkClientWrapper getClientWrapper() {
        return new FlinkClientWrapperImpl();
    }

    @Override
    public void close() {}
}
