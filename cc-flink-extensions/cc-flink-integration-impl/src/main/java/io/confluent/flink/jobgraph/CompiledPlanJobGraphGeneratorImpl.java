/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.jobgraph;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.planner.delegation.DefaultExecutor;
import org.apache.flink.table.planner.delegation.PlannerBase;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;

/** CompiledPlan-based {@link JobGraphGenerator} implementation. */
public class CompiledPlanJobGraphGeneratorImpl implements JobGraphGenerator {
    @Override
    public JobGraphWrapper generateJobGraph(List<String> arguments, Downloader downloader) {
        Configuration configuration = new Configuration();
        ClassLoader loader = CompiledPlanJobGraphGeneratorImpl.class.getClassLoader();

        String compiledPlan = arguments.get(0);
        TableEnvironmentImpl tableEnvironment =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance()
                                .withConfiguration(configuration)
                                .withClassLoader(loader)
                                .build());
        PlannerBase planner = (PlannerBase) tableEnvironment.getPlanner();

        // explicitly configure a parallelism of 1 because otherwise the number of cpu cores is
        // used as the default
        StreamExecutionEnvironment streamExecutionEnvironment =
                planner.getExecEnv().setParallelism(1);
        Executor execEnv = new DefaultExecutor(streamExecutionEnvironment);
        PlanReference planReference = PlanReference.fromJsonString(compiledPlan);
        InternalPlan plan;
        try {
            plan = tableEnvironment.getPlanner().loadPlan(planReference);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<Transformation<?>> transformations = planner.translatePlan(plan);
        Pipeline pipeline = execEnv.createPipeline(transformations, configuration, null);

        try {
            return new JobGraphWrapperImpl(
                    PipelineExecutorUtils.getJobGraph(pipeline, configuration, loader));
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
