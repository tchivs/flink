/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** {@link JobGraphWrapper} implementation. */
public class JobGraphWrapperImpl implements JobGraphWrapper {

    private final JobGraph jobGraph;

    public JobGraphWrapperImpl(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
    }

    @Override
    public Collection<JobVertexContainer> getJobVertices() {
        return StreamSupport.stream(jobGraph.getVertices().spliterator(), false)
                .map(
                        vertex ->
                                new JobVertexContainer(
                                        vertex.getID().toHexString(), vertex.getMaxParallelism()))
                .collect(Collectors.toList());
    }

    @Override
    public void handleRequirements(JobRequirementsContainer jobRequirementsContainer) {
        // no-op for now until AUTO-54
    }

    @Override
    public void setSavepointPath(String sp) {
        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(sp));
    }

    @Override
    public void setJobID(String jobId) {
        jobGraph.setJobID(JobID.fromHexString(jobId));
    }

    @Override
    public Object unwrapJobGraph() {
        return jobGraph;
    }
}
