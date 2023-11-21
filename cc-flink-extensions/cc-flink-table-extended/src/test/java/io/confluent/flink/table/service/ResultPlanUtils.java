/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.operations.QueryOperation;

import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import io.confluent.flink.table.service.ServiceTasks.Service;

import java.util.Collections;

/** Test utilities to create a {@link ResultPlan}. */
@Confluent
public class ResultPlanUtils {

    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    public static ForegroundJobResultPlan foregroundJobCustomConfig(
            TableEnvironment tableEnv, String sql) throws Exception {
        final QueryOperation queryOperation = tableEnv.sqlQuery(sql).getQueryOperation();
        return (ForegroundJobResultPlan)
                INSTANCE.compileForegroundQuery(
                        tableEnv,
                        queryOperation,
                        (identifier, execNodeId) -> Collections.emptyMap());
    }

    public static ForegroundJobResultPlan foregroundJob(TableEnvironment tableEnv, String sql)
            throws Exception {
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), Collections.emptyMap(), Service.SQL_SERVICE);
        final QueryOperation queryOperation = tableEnv.sqlQuery(sql).getQueryOperation();
        return (ForegroundJobResultPlan)
                INSTANCE.compileForegroundQuery(
                        tableEnv,
                        queryOperation,
                        (identifier, execNodeId) -> Collections.emptyMap());
    }
}
