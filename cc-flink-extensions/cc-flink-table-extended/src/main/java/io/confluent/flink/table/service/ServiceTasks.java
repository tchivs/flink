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
     * Specifies the caller side for methods that have slightly different behavior depending on the
     * service.
     */
    enum Service {
        SQL_SERVICE,
        JOB_SUBMISSION_SERVICE
    }

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
     *
     * @param publicOptions map of {@link ServiceTasksOptions#ALL_PUBLIC_OPTIONS} to be validated
     *     and applied
     * @param privateOptions map of {@link ServiceTasksOptions} with {@link
     *     ServiceTasksOptions#PRIVATE_PREFIX} to be applied
     * @return prepared {@link ServiceTasksOptions} to be persisted in resources (i.e. private
     *     options stay as they are and public options get prefixed with {@link
     *     ServiceTasksOptions#PRIVATE_USER_PREFIX})
     */
    Map<String, String> configureEnvironment(
            TableEnvironment tableEnvironment,
            Map<String, String> publicOptions,
            Map<String, String> privateOptions,
            Service service);

    /**
     * Compiles a {@link QueryOperation} (i.e. a SELECT statement) for foreground result serving.
     */
    ForegroundResultPlan compileForegroundQuery(
            TableEnvironment tableEnvironment,
            QueryOperation queryOperation,
            ConnectorOptionsMutator connectorOptions)
            throws Exception;

    /**
     * Compiles one or more {@link ModifyOperation}s (i.e. an INSERT INTO or STATEMENT SET) for
     * background queries.
     */
    BackgroundJobResultPlan compileBackgroundQueries(
            TableEnvironment tableEnvironment,
            List<ModifyOperation> modifyOperations,
            ConnectorOptionsMutator connectorOptions)
            throws Exception;
}
