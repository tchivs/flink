/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.operations.QueryOperation;

import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultServiceTasks}. */
@Confluent
public class DefaultServiceTasksTest {

    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    @Test
    void testCompileForegroundQuery() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final QueryOperation queryOperation =
                tableEnv.sqlQuery("SELECT * FROM (VALUES (1), (2), (3))").getQueryOperation();

        final ForegroundResultPlan plan = INSTANCE.compileForegroundQuery(tableEnv, queryOperation);
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }
}
