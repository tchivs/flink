/*
 * Copyright 2023 Confluent Inc.
 */
package io.confluent.flink.jobgraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectClusterRESTAddress;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;

class FlinkClientWrapperImplTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            new MiniClusterExtension(new MiniClusterResourceConfiguration.Builder().build());

    @Test
    void testSubmitJob(
            @InjectClusterRESTAddress URI restAddress,
            @InjectClusterClient ClusterClient<?> clusterClient)
            throws Exception {
        final FlinkClientWrapperImpl clientWrapper = new FlinkClientWrapperImpl();

        final JobGraphWrapper jobGraphWrapper =
                new JobGraphWrapperImpl(JobGraphTestUtils.singleNoOpJobGraph());

        final JobID jobId = JobID.generate();
        jobGraphWrapper.setJobID(jobId.toHexString());

        final JobManagerLocation jobManagerLocation =
                new JobManagerLocation(restAddress.getHost(), restAddress.getPort());

        clientWrapper.submitJobGraph(jobGraphWrapper, jobManagerLocation).get();

        // verify that the job truly was submitted to the cluster
        // the actual job status doesn't matter, so long as we could query it
        clusterClient.getJobStatus(jobId).get();
    }
}
