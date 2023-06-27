/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v2;

import io.confluent.flink.jobgraph.VersionedFlinkIntegration;

import java.io.Closeable;

/** An abstraction around Flink Client. The purpose is to encapsulate Flink Client version. */
public interface FlinkIntegrationV2 extends Closeable, VersionedFlinkIntegration {

    @Override
    default int getVersion() {
        return 2;
    }

    JobGraphGeneratorV2 getJobGraphGenerator();

    FlinkClientWrapperV2 getFlinkClientWrapper();
}
