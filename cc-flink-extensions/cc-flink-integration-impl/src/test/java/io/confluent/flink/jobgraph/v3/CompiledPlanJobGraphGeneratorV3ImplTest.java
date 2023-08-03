/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v3;

import org.junit.jupiter.api.Test;

import java.util.Collections;

class CompiledPlanJobGraphGeneratorV3ImplTest {

    private static final String PRELOAD_PLAN =
            CompiledPlanJobGraphGeneratorV3Impl.loadPreloadPlanResource();

    @Test
    void testPreloadThrowsNoException() {
        new CompiledPlanJobGraphGeneratorV3Impl().prepareGeneration();
    }

    @Test
    void testJobGraphGenerationSucceedsWithoutPreload() {
        new CompiledPlanJobGraphGeneratorV3Impl()
                .generateJobGraph(Collections.singletonList(PRELOAD_PLAN), Collections.emptyMap());
    }

    @Test
    void testJobGraphGenerationSucceedsWithPreload() {
        CompiledPlanJobGraphGeneratorV3Impl compiledPlanJobGraphGeneratorV3 =
                new CompiledPlanJobGraphGeneratorV3Impl();
        compiledPlanJobGraphGeneratorV3.prepareGeneration();
        compiledPlanJobGraphGeneratorV3.generateJobGraph(
                Collections.singletonList(PRELOAD_PLAN), Collections.emptyMap());
    }

    @Test
    void testJobGraphGenerationMayBeCalledMultipleTimes() {
        CompiledPlanJobGraphGeneratorV3Impl compiledPlanJobGraphGeneratorV3 =
                new CompiledPlanJobGraphGeneratorV3Impl();
        compiledPlanJobGraphGeneratorV3.prepareGeneration();

        for (int x = 0; x < 5; x++) {
            compiledPlanJobGraphGeneratorV3.generateJobGraph(
                    Collections.singletonList(PRELOAD_PLAN), Collections.emptyMap());
        }
    }
}
