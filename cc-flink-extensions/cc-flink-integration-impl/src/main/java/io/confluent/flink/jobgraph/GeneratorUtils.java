/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.planner.delegation.DefaultExecutor;
import org.apache.flink.table.planner.delegation.PlannerBase;

import io.confluent.flink.table.service.ServiceTasks;
import io.confluent.flink.table.service.ServiceTasks.Service;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.confluent.flink.table.service.ServiceTasksOptions.CONFLUENT_AI_FUNCTIONS_ENABLED;
import static io.confluent.flink.table.service.ServiceTasksOptions.PRIVATE_USER_PREFIX;

/** Utils for {@link JobGraph} generation. Currently, only from a {@link CompiledPlan}. */
public class GeneratorUtils {

    /**
     * @param arguments string-based arguments, currently only {@link CompiledPlan}.
     * @param allOptions includes Flink cluster configuration, Flink job configuration, and
     *     Confluent-specific job options. It assumes that Confluent-specific options are prefixed
     *     accordingly to avoid any naming conflicts with existing or future Flink options.
     */
    public static JobGraphWrapper generateJobGraph(
            List<String> arguments, Map<String, String> allOptions) {
        final String compiledPlan = arguments.get(0);

        final Configuration flinkConfiguration = Configuration.fromMap(allOptions);
        final ClassLoader loader = GeneratorUtils.class.getClassLoader();

        final TableEnvironmentImpl tableEnvironment =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance()
                                .withConfiguration(flinkConfiguration)
                                .withClassLoader(loader)
                                .build());

        // Public user options are only passed here to ensure they don't conflict with Flink
        // options. Private options and Flink options are used to configure the TableEnvironment.
        final Map<String, String> publicOptions =
                allOptions.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(PRIVATE_USER_PREFIX))
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().substring(PRIVATE_USER_PREFIX.length()),
                                        Map.Entry::getValue));

        // TODO remove this and let configureEnvironment() do its job
        // Always enable AI functions for JSS, SQL service will guard the CompiledPlan reference
        publicOptions.put(CONFLUENT_AI_FUNCTIONS_ENABLED.key(), "true");

        ServiceTasks.INSTANCE.configureEnvironment(
                tableEnvironment,
                publicOptions,
                Collections.emptyMap(),
                Service.JOB_SUBMISSION_SERVICE);

        final PlannerBase planner = (PlannerBase) tableEnvironment.getPlanner();

        final StreamExecutionEnvironment streamExecutionEnvironment = planner.getExecEnv();

        // Explicitly configure a parallelism of 1 because otherwise the number of cpu cores is
        // used as the default.
        streamExecutionEnvironment.setParallelism(1);

        final Executor execEnv = new DefaultExecutor(streamExecutionEnvironment);
        final PlanReference planReference = PlanReference.fromJsonString(compiledPlan);
        final InternalPlan plan;
        try {
            plan = tableEnvironment.getPlanner().loadPlan(planReference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final List<Transformation<?>> transformations = planner.translatePlan(plan);
        final Pipeline pipeline = execEnv.createPipeline(transformations, flinkConfiguration, null);
        try {
            return new JobGraphWrapperImpl(
                    PipelineExecutorUtils.getJobGraph(pipeline, flinkConfiguration, loader));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
