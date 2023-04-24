/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

/** A part of input to Flink Autopilot to decide on Job parallelism. */
class JobVertexContainer {
    private final String vertexId;

    private final int maxParallelism;

    JobVertexContainer(String vertexId, int maxParallelism) {
        this.vertexId = vertexId;
        this.maxParallelism = maxParallelism;
    }

    public String getVertexId() {
        return vertexId;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }
}
