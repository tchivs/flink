/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import java.io.Closeable;

/** Facade interface for generating and submitting Flink job graphs. */
public interface FlinkIntegration extends Closeable {
    JobGraphGenerator getJobGraphGenerator();

    FlinkClientWrapper getClientWrapper();
}
