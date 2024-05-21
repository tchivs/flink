/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.planner.delegation.DefaultExecutor;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan;
import org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import io.confluent.flink.table.service.ServiceTasks;
import io.confluent.flink.table.utils.ClassifiedException;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.confluent.flink.table.service.ServiceTasksOptions.PRIVATE_USER_PREFIX;

/** Utils for the {@link ConfluentJobSubmitHandler}. */
@Confluent
class ConfluentGeneratorUtils {

    static JobGraph generateJobGraph(
            String compiledPlan, String tableOptionsOverrides, Map<String, String> allOptions)
            throws Exception {
        final Configuration allConfig = Configuration.fromMap(allOptions);
        final ClassLoader loader = ConfluentJobSubmitHandler.class.getClassLoader();

        final TableEnvironmentImpl tableEnvironment =
                TableEnvironmentImpl.create(
                        EnvironmentSettings.newInstance()
                                .withConfiguration(allConfig)
                                .withClassLoader(loader)
                                .build());

        // Public user options are extracted here to not conflict with Flink options.
        final Map<String, String> publicOptions =
                allOptions.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(PRIVATE_USER_PREFIX))
                        .collect(
                                Collectors.toMap(
                                        e -> e.getKey().substring(PRIVATE_USER_PREFIX.length()),
                                        Map.Entry::getValue));

        ServiceTasks.INSTANCE.configureEnvironment(
                tableEnvironment,
                publicOptions,
                allOptions,
                ServiceTasks.Service.JOB_SUBMISSION_SERVICE);

        final PlannerBase planner = (PlannerBase) tableEnvironment.getPlanner();

        final StreamExecutionEnvironment streamExecutionEnvironment = planner.getExecEnv();

        // Explicitly configure a parallelism of 1 because otherwise the number of cpu cores is
        // used as the default.
        streamExecutionEnvironment.setParallelism(1);

        final org.apache.flink.table.delegation.Executor execEnv =
                new DefaultExecutor(streamExecutionEnvironment);
        final PlanReference planReference = PlanReference.fromJsonString(compiledPlan);
        final InternalPlan plan = tableEnvironment.getPlanner().loadPlan(planReference);

        // Apply table options overrides to the plan if present.
        if (!Strings.isNullOrEmpty(tableOptionsOverrides)) {
            final Map<String, Map<String, String>> optionsOverrides =
                    JsonSerdeUtil.createObjectReader(planner.createSerdeContext())
                            .readValue(tableOptionsOverrides, Map.class);

            PlanOverrides.updateTableOptions((ExecNodeGraphInternalPlan) plan, optionsOverrides);
        }

        final List<Transformation<?>> transformations = planner.translatePlan(plan);
        final Pipeline pipeline = execEnv.createPipeline(transformations, allConfig, null);

        return PipelineExecutorUtils.getJobGraph(pipeline, allConfig, loader);
    }

    static RestHandlerException convertJobGraphGenerationException(Throwable e) {
        final ClassifiedException classified =
                ClassifiedException.of(e, ClassifiedException.VALID_CAUSES, new Configuration());
        if (classified.getKind() == ClassifiedException.ExceptionKind.USER) {
            return new RestHandlerException(
                    classified.getSensitiveMessage(),
                    HttpResponseStatus.BAD_REQUEST,
                    RestHandlerException.LoggingBehavior.IGNORE);
        } else {
            ConfluentJobSubmitHandler.LOG.error(
                    "Internal error occurred during JobGraph generation.",
                    classified.getLogException());
            return new RestHandlerException(
                    "Internal error occurred.", HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
