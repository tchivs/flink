/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

class CompiledPlanJobGraphGeneratorImplIntegrationTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            new MiniClusterExtension(new MiniClusterResourceConfiguration.Builder().build());

    @Test
    void testGenerateJobGraph(@InjectClusterClient ClusterClient<?> client) throws Exception {
        // the existence of the MiniClusterExtension can affect the job graph generation
        // since it sets of a context stream execution environment
        final JobGraphWrapper jobGraphWrapper = CompiledPlanJobGraphGeneratorImplTest.compileJob();

        final JobID jobId = client.submitJob((JobGraph) jobGraphWrapper.unwrapJobGraph()).get();
        final JobResult jobResult = client.requestJobResult(jobId).get();
        assertThat(jobResult.getApplicationStatus()).isSameAs(ApplicationStatus.SUCCEEDED);
    }
}
