/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v3;

import org.apache.flink.annotation.VisibleForTesting;

import io.confluent.flink.jobgraph.GeneratorUtils;
import io.confluent.flink.jobgraph.JobGraphGenerator;
import io.confluent.flink.jobgraph.JobGraphWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** CompiledPlan-based {@link JobGraphGenerator} implementation. */
public class CompiledPlanJobGraphGeneratorV3Impl implements JobGraphGeneratorV3 {

    @Override
    public void prepareGeneration() {
        final String compiledPlan = loadPreloadPlanResource();
        generateJobGraph(Collections.singletonList(compiledPlan), Collections.emptyMap());
    }

    @Override
    public JobGraphWrapper generateJobGraph(
            List<String> arguments, Map<String, String> allOptions) {
        return GeneratorUtils.generateJobGraph(arguments, allOptions);
    }

    @VisibleForTesting
    static String loadPreloadPlanResource() {
        try {
            String resource =
                    CompiledPlanJobGraphGeneratorV3Impl.class
                            .getResource("/preload_plan.json")
                            .getPath();
            return new String(Files.readAllBytes(Paths.get(resource)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
