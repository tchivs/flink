/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v3;

import org.apache.flink.annotation.VisibleForTesting;

import io.confluent.flink.jobgraph.GeneratorUtils;
import io.confluent.flink.jobgraph.JobGraphGenerator;
import io.confluent.flink.jobgraph.JobGraphWrapper;
import io.confluent.flink.table.utils.ClassifiedException;
import io.confluent.flink.table.utils.ClassifiedException.ExceptionClass;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** CompiledPlan-based {@link JobGraphGenerator} implementation. */
public class CompiledPlanJobGraphGeneratorV3Impl implements JobGraphGeneratorV3 {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CompiledPlanJobGraphGeneratorV3Impl.class);

    @Override
    public void prepareGeneration() {
        final String compiledPlan = loadPreloadPlanResource();
        generateJobGraph(Collections.singletonList(compiledPlan), Collections.emptyMap());
    }

    @Override
    public JobGraphWrapper generateJobGraph(
            List<String> arguments, Map<String, String> allOptions) {
        try {
            return GeneratorUtils.generateJobGraph(arguments, allOptions);
        } catch (Exception e) {
            final ClassifiedException classified =
                    ClassifiedException.of(e, ClassifiedException.VALID_CAUSES);
            if (classified.getExceptionClass() == ExceptionClass.PLANNING_USER) {
                throw new RuntimeException(classified.getMessage());
            } else {
                LOGGER.error("Internal error occurred during JobGraph generation.", e);
                throw new RuntimeException("Internal error occurred.");
            }
        }
    }

    @VisibleForTesting
    static String loadPreloadPlanResource() {
        try {
            return IOUtils.toString(
                    Objects.requireNonNull(
                            CompiledPlanJobGraphGeneratorV3Impl.class.getResourceAsStream(
                                    "/preload_plan.json")),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
