/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v2;

import io.confluent.flink.jobgraph.JobGraphGenerator;
import io.confluent.flink.jobgraph.JobGraphWrapper;
import io.confluent.flink.jobgraph.v3.CompiledPlanJobGraphGeneratorV3Impl;

import java.util.List;
import java.util.Map;

/** CompiledPlan-based {@link JobGraphGenerator} implementation. */
@Deprecated
public class CompiledPlanJobGraphGeneratorV2Impl implements JobGraphGeneratorV2 {

    private static final CompiledPlanJobGraphGeneratorV3Impl DELEGATE =
            new CompiledPlanJobGraphGeneratorV3Impl();

    @Override
    public JobGraphWrapper generateJobGraph(
            List<String> arguments, Map<String, String> generatorConfiguration) {
        return DELEGATE.generateJobGraph(arguments, generatorConfiguration);
    }
}
