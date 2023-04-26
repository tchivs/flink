/*
 * Copyright 2023 Confluent Inc.
 */
package io.confluent.flink.jobgraph;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class CompiledPlanJobGraphGeneratorImplTest {

    /**
     * Verifies that the generated job graph uses a default parallelism of 1.
     *
     * <p>This test MUST NOT be run with a MiniClusterExtension because it affects the generated
     * graph.
     */
    @Test
    void testGeneratedJobGraphHasParallelismOf1() throws Exception {
        final JobGraphWrapper jobGraphWrapper = compileJob();

        final JobGraph jobGraph = (JobGraph) jobGraphWrapper.unwrapJobGraph();

        assertThat(jobGraph.getMaximumParallelism()).isEqualTo(1);
    }

    static JobGraphWrapper compileJob() throws IOException {
        final String compiledPlan =
                Files.readString(
                        Paths.get(
                                Objects.requireNonNull(
                                                CompiledPlanJobGraphGeneratorImpl.class.getResource(
                                                        "/compiled_plan.json"))
                                        .getPath()));

        final JobGraphGenerator jobGraphGenerator = new CompiledPlanJobGraphGeneratorImpl();

        return jobGraphGenerator.generateJobGraph(Collections.singletonList(compiledPlan), null);
    }
}
