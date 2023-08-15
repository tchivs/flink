/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;

import java.util.List;
import java.util.Map;

/**
 * Provides abstractions that can be used by the SQL Service or Job Submission Service (JSS) without
 * depending on internal Flink classes. Since Flink evolves over time it is saver to detect breaking
 * changes of internal classes here instead of downstream repositories.
 *
 * <p>The method signatures should remain stable across Flink versions. Otherwise, changes in the
 * SQL or Job Submission Service are necessary.
 */
@Confluent
public interface ServiceTasks {

    ServiceTasks INSTANCE = new DefaultServiceTasks();

    /**
     * Applies configuration options to the given {@link TableEnvironment}.
     *
     * <p>Since configuration is not part of the {@link CompiledPlan}, this method is called from
     * both SQL Service and Job Submission Service.
     *
     * <p>For SQL service, the given options must adhere to {@link ServiceTasksOptions}, metastore
     * might be accessed, and validation of the entire "session" is performed.
     *
     * <p>For Job Submission Service, validation is not performed for enabling cases where
     * customizing Flink with low-level options is necessary. Also, metastore cannot be accessed in
     * JSS.
     */
    void configureEnvironment(
            TableEnvironment tableEnvironment,
            Map<String, String> options,
            boolean validateSession);

    /**
     * Compiles a {@link QueryOperation} (i.e. a SELECT statement) for foreground result serving.
     */
    ForegroundResultPlan compileForegroundQuery(
            TableEnvironment tableEnvironment,
            QueryOperation queryOperation,
            ConnectorOptionsProvider connectorOptions)
            throws Exception;

    /**
     * Compiles one or more {@link ModifyOperation}s (i.e. an INSERT INTO or STATEMENT SET) for
     * background queries.
     */
    BackgroundResultPlan compileBackgroundQueries(
            TableEnvironment tableEnvironment,
            List<ModifyOperation> modifyOperations,
            ConnectorOptionsProvider connectorOptions)
            throws Exception;
}
