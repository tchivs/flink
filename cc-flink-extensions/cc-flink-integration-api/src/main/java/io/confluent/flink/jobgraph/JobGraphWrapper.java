/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import java.util.Collection;
import java.util.Map;

/** A wrapper around Flink JobGraph (of some version). */
public interface JobGraphWrapper {

    /** Should iterate over vertices - so that Autopilot Service can make a decision. */
    Collection<JobVertexContainer> getJobVertices();

    /** Should call Flink {@code JobResourceRequirements.writeToJobGraph(graph, req)}. */
    void handleRequirements(JobRequirementsContainer jobRequirementsContainer);

    /** Should call Flink {@code graph.setSavepointRestoreSettings(...)}. */
    void setSavepointPath(String sp);

    /** Should call Flink {@code graph.setJobID(...)}. */
    void setJobID(String jobId);

    /**
     * @return unwrapped instance of {@code org.apache.flink.runtime.jobgraph.JobGraph} used to
     *     actually submit the job for execution
     */
    Object unwrapJobGraph();

    /** Should call {@code jobGraph.getJobConfiguration.setString} for each entry. */
    void setConfiguration(Map<String, String> map);
}
