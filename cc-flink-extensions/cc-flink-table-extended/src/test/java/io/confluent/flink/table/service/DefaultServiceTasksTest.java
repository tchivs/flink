/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;

import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

        final ForegroundResultPlan plan =
                INSTANCE.compileForegroundQuery(
                        tableEnv,
                        queryOperation,
                        (identifier, execNodeId) -> Collections.emptyMap());
        assertThat(plan.getCompiledPlan()).contains(ForegroundResultTableFactory.IDENTIFIER);
    }

    @Test
    void testCompileBackgroundQueries() throws Exception {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        final Catalog catalog =
                tableEnv.getCatalog(tableEnv.getCurrentCatalog())
                        .orElseThrow(IllegalArgumentException::new);

        final Schema schema = Schema.newBuilder().column("i", "INT").build();
        final Map<String, String> privateOptions =
                Collections.singletonMap("confluent.specific", "option");

        catalog.createTable(
                new ObjectPath(tableEnv.getCurrentDatabase(), "source"),
                new ConfluentCatalogTable(
                        schema,
                        null,
                        Collections.emptyList(),
                        Collections.singletonMap("connector", "datagen"),
                        privateOptions),
                false);

        catalog.createTable(
                new ObjectPath(tableEnv.getCurrentDatabase(), "sink"),
                new ConfluentCatalogTable(
                        schema,
                        null,
                        Collections.emptyList(),
                        Collections.singletonMap("connector", "blackhole"),
                        privateOptions),
                false);

        final List<Operation> operations =
                ((TableEnvironmentImpl) tableEnv)
                        .getPlanner()
                        .getParser()
                        .parse("INSERT INTO sink SELECT * FROM source");
        final SinkModifyOperation modifyOperation = (SinkModifyOperation) operations.get(0);
        assertThat(modifyOperation.getContextResolvedTable().getTable().getOptions())
                .doesNotContainKey("confluent.specific");

        final ConnectorOptionsProvider optionsProvider =
                (identifier, execNodeId) -> {
                    // execNodeId is omitted because it is not deterministic
                    return Collections.singletonMap(
                            "transactional-id", "my_" + identifier.getObjectName());
                };

        final BackgroundResultPlan plan =
                INSTANCE.compileBackgroundQueries(
                        tableEnv, Collections.singletonList(modifyOperation), optionsProvider);

        assertThat(plan.getCompiledPlan().replaceAll("[\\s\"]", ""))
                .contains(
                        "options:{connector:datagen,confluent.specific:option,transactional-id:my_source}")
                .contains(
                        "options:{connector:blackhole,confluent.specific:option,transactional-id:my_sink}");
    }

    @Test
    void testConfigurationNonDeterminism() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(tableEnv);

        final QueryOperation queryOperation =
                tableEnv.sqlQuery("SELECT NOW(), COUNT(*) FROM (VALUES (1), (2), (3))")
                        .getQueryOperation();

        assertThatThrownBy(
                        () ->
                                INSTANCE.compileForegroundQuery(
                                        tableEnv,
                                        queryOperation,
                                        (identifier, execNodeId) -> Collections.emptyMap()))
                .hasMessageContaining("can not satisfy the determinism requirement");
    }
}
