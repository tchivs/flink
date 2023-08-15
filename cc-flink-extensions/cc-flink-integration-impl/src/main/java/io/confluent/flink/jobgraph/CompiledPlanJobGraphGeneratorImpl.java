/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import io.confluent.flink.jobgraph.v2.CompiledPlanJobGraphGeneratorV2Impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * CompiledPlan-based {@link JobGraphGenerator} implementation.
 *
 * @deprecated Use the versioned one instead.
 */
@Deprecated
public class CompiledPlanJobGraphGeneratorImpl implements JobGraphGenerator {

    private static final CompiledPlanJobGraphGeneratorV2Impl DELEGATE =
            new CompiledPlanJobGraphGeneratorV2Impl();

    @Override
    public JobGraphWrapper generateJobGraph(
            List<String> arguments, Downloader downloader, Map<String, String> metaInfo) {
        return DELEGATE.generateJobGraph(arguments, Collections.emptyMap());
    }
}
