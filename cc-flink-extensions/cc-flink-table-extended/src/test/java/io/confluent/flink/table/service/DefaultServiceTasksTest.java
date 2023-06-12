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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;

import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

        final BackgroundResultPlan plan =
                INSTANCE.compileBackgroundQueries(
                        tableEnv, Collections.singletonList((ModifyOperation) operations.get(0)));

        assertThat(plan.getCompiledPlan())
                .doesNotContain("datagen")
                .doesNotContain("blackhole")
                .doesNotContain("confluent.specific");

        final Map<String, String> expectedSourceMetadata = new HashMap<>();
        expectedSourceMetadata.put("connector", "datagen");
        expectedSourceMetadata.putAll(privateOptions);

        final Map<String, String> expectedSinkMetadata = new HashMap<>();
        expectedSinkMetadata.put("connector", "blackhole");
        expectedSinkMetadata.putAll(privateOptions);

        assertThat(plan.getConnectorMetadata())
                .containsExactlyInAnyOrder(
                        new ConnectorMetadata(
                                ObjectIdentifier.of(
                                        tableEnv.getCurrentCatalog(),
                                        tableEnv.getCurrentDatabase(),
                                        "source"),
                                expectedSourceMetadata),
                        new ConnectorMetadata(
                                ObjectIdentifier.of(
                                        tableEnv.getCurrentCatalog(),
                                        tableEnv.getCurrentDatabase(),
                                        "sink"),
                                expectedSinkMetadata));
    }
}
