/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import java.util.concurrent.Future;

/** An abstraction around Flink Client. The purpose is to encapsulate Flink Client version. */
interface FlinkClientWrapper {
    /**
     * Submit the given JobGraph for execution. e.g. by calling <a
     * href="https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/client/program/ClusterClient.html#submitJob-org.apache.flink.runtime.jobgraph.JobGraph-">ClusterClient.submitJob</a>
     *
     * @param jobManagerLocation provided by the Flink Control Plane
     */
    Future<?> submitJobGraph(
            JobGraphWrapper jobGraphWrapper, JobManagerLocation jobManagerLocation);
}
