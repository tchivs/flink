/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v2;

import io.confluent.flink.jobgraph.JobGraphWrapper;
import io.confluent.flink.jobgraph.JobManagerLocation;

import java.util.concurrent.CompletableFuture;

/** An abstraction around Flink Client. The purpose is to encapsulate Flink Client version. */
public interface FlinkClientWrapperV2 {

    /**
     * Submit the given JobGraph for execution. e.g. by calling <a
     * href="https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/client/program/ClusterClient.html#submitJob-org.apache.flink.runtime.jobgraph.JobGraph-">ClusterClient.submitJob</a>
     *
     * @param jobManagerLocation provided by the Flink Control Plane
     */
    CompletableFuture<?> submitJobGraph(
            JobGraphWrapper jobGraphWrapper, JobManagerLocation jobManagerLocation);
}
