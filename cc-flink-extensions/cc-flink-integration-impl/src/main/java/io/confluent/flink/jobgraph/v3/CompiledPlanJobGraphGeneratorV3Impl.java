/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph.v3;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;

import io.confluent.flink.jobgraph.GeneratorUtils;
import io.confluent.flink.jobgraph.JobGraphGenerator;
import io.confluent.flink.jobgraph.JobGraphWrapper;
import io.confluent.flink.table.utils.ClassifiedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
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
        try {
            return GeneratorUtils.generateJobGraph(arguments, allOptions);
        } catch (Exception e) {
            // Rebuild error message that is shown to the user:
            // - format/add more data in case of ClassifiedException.ExceptionClass.PLANNING_USER
            // - hide sensitive data in case of ClassifiedException.ExceptionClass.PLANNING_SYSTEM
            String userFacingErrorMessage =
                    ClassifiedException.of(
                                    e,
                                    new HashSet<>(
                                            Arrays.asList(
                                                    TableException.class,
                                                    ValidationException.class,
                                                    SqlValidateException.class,
                                                    DatabaseNotExistException.class,
                                                    TableNotExistException.class,
                                                    TableAlreadyExistException.class)))
                            .getMessage();
            throw new RuntimeException(userFacingErrorMessage, e);
        }
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
