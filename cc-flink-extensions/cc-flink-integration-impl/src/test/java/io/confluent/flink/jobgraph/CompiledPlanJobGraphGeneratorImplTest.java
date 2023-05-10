/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

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
        final String compiledPlan = loadResource("/compiled_plan.json");

        final JobGraphGenerator jobGraphGenerator = new CompiledPlanJobGraphGeneratorImpl();

        return jobGraphGenerator.generateJobGraph(
                Collections.singletonList(compiledPlan), null, Collections.emptyMap());
    }

    private static String loadResource(String name) throws IOException {
        InputStream in = CompiledPlanJobGraphGeneratorImpl.class.getResourceAsStream(name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyBytes(in, out);
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
