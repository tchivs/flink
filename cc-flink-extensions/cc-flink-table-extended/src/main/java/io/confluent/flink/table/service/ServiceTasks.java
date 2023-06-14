/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;

import java.util.List;

/**
 * Provides abstractions that can be used by the SQL Service without depending on internal Flink
 * classes. Since Flink evolves over time it is saver to detect breaking changes of internal classes
 * here instead of downstream repositories.
 *
 * <p>The method signatures should remain stable across Flink versions. Otherwise, changes in the
 * SQL Service are necessary.
 */
@Confluent
public interface ServiceTasks {

    ServiceTasks INSTANCE = new DefaultServiceTasks();

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
