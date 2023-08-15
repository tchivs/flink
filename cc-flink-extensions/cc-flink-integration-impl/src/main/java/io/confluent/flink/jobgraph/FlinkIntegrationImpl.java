/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import org.apache.flink.runtime.util.EnvironmentInformation;

import io.confluent.flink.jobgraph.v2.FlinkClientWrapperV2Impl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link FlinkIntegration} implementation.
 *
 * @deprecated Use the versioned one instead.
 */
@Deprecated
public class FlinkIntegrationImpl implements FlinkIntegration {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkIntegrationImpl.class);

    public FlinkIntegrationImpl() {
        LOG.info("Created FlinkIntegration for version {}.", EnvironmentInformation.getVersion());
    }

    @Override
    public JobGraphGenerator getJobGraphGenerator() {
        return new CompiledPlanJobGraphGeneratorImpl();
    }

    @Override
    public FlinkClientWrapper getClientWrapper() {
        return new FlinkClientWrapperV2Impl();
    }

    @Override
    public void close() {}
}
