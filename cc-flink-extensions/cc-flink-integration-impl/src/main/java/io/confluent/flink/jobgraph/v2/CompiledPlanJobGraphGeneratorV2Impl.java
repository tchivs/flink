/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v2;

import io.confluent.flink.jobgraph.GeneratorUtils;
import io.confluent.flink.jobgraph.JobGraphGenerator;
import io.confluent.flink.jobgraph.JobGraphWrapper;

import java.util.List;
import java.util.Map;

/** CompiledPlan-based {@link JobGraphGenerator} implementation. */
public class CompiledPlanJobGraphGeneratorV2Impl implements JobGraphGeneratorV2 {

    @Override
    public JobGraphWrapper generateJobGraph(
            List<String> arguments, Map<String, String> generatorConfiguration) {
        return GeneratorUtils.generateJobGraph(arguments, generatorConfiguration);
    }
}
