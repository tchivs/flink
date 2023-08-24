/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v3;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void testJobGraphGenerationForwardsUserOptions() {
        CompiledPlanJobGraphGeneratorV3Impl compiledPlanJobGraphGeneratorV3 =
                new CompiledPlanJobGraphGeneratorV3Impl();

        // Passes because the Confluent option is not prefixed and unknown to Flink
        compiledPlanJobGraphGeneratorV3.generateJobGraph(
                Collections.singletonList(PRELOAD_PLAN),
                Collections.singletonMap("sql.state-ttl", "INVALID"));

        // Errors because the Confluent option is prefixed and is converted to a Flink option
        // which fails
        assertThatThrownBy(
                        () ->
                                compiledPlanJobGraphGeneratorV3.generateJobGraph(
                                        Collections.singletonList(PRELOAD_PLAN),
                                        Collections.singletonMap(
                                                "confluent.user.sql.state-ttl", "INVALID")))
                .hasMessageContaining("Could not parse value 'INVALID' for key 'sql.state-ttl'.");
    }

    @Test
    void testJobGraphGenerationClassifiesExceptions() {
        CompiledPlanJobGraphGeneratorV3Impl compiledPlanJobGraphGeneratorV3 =
                new CompiledPlanJobGraphGeneratorV3Impl();

        // system planning error should be reported mostly as is:
        assertThatThrownBy(
                        () ->
                                compiledPlanJobGraphGeneratorV3.generateJobGraph(
                                        Collections.singletonList("invalid plan reference"),
                                        Collections.emptyMap()))
                .hasMessageContaining("invalid plan reference");
    }
}
