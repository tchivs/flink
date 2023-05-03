/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JobGraphWrapperImplTest {

    @Test
    void getJobVertices() {
        final JobVertex vertex1 = new JobVertex("vertex1");
        vertex1.setMaxParallelism(4);
        final JobVertex vertex2 = new JobVertex("vertex1");
        vertex1.setMaxParallelism(8);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex1, vertex2);
        final JobGraphWrapperImpl jobGraphWrapper = new JobGraphWrapperImpl(jobGraph);

        assertThat(jobGraphWrapper.getJobVertices())
                .hasSize(2)
                .anySatisfy(
                        container -> {
                            assertThat(container.getVertexId())
                                    .isEqualTo(vertex1.getID().toHexString());
                            assertThat(container.getMaxParallelism())
                                    .isEqualTo(vertex1.getMaxParallelism());
                        })
                .anySatisfy(
                        container -> {
                            assertThat(container.getVertexId())
                                    .isEqualTo(vertex2.getID().toHexString());
                            assertThat(container.getMaxParallelism())
                                    .isEqualTo(vertex2.getMaxParallelism());
                        });
    }

    @Test
    void setSavepointPath() {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        final JobGraphWrapperImpl jobGraphWrapper = new JobGraphWrapperImpl(jobGraph);
        final String savepointPath = "some path";

        jobGraphWrapper.setSavepointPath(savepointPath);

        assertThat(jobGraph.getSavepointRestoreSettings().getRestorePath())
                .isEqualTo(savepointPath);
    }

    @Test
    void setJobID() {
        final JobID jobId = JobID.generate();
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        final JobGraphWrapperImpl jobGraphWrapper = new JobGraphWrapperImpl(jobGraph);

        jobGraphWrapper.setJobID(jobId.toHexString());

        assertThat(jobGraph.getJobID()).isEqualTo(jobId);
    }

    @Test
    void unwrapJobGraph() {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        final JobGraphWrapperImpl jobGraphWrapper = new JobGraphWrapperImpl(jobGraph);

        assertThat(jobGraphWrapper.unwrapJobGraph()).isSameAs(jobGraph);
    }
}
