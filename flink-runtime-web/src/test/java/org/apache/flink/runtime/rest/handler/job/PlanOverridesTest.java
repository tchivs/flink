/*
 * Copyright 2024 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PlanOverrides}. */
@Confluent
public class PlanOverridesTest {
    private static final TableEnvironmentImpl TABLE_ENVIRONMENT =
            TableEnvironmentImpl.create(
                    EnvironmentSettings.newInstance()
                            .withClassLoader(PlanOverridesTest.class.getClassLoader())
                            .build());

    @Test
    public void testUpdateTableOptions() throws Exception {
        final ExecNodeGraphInternalPlan plan = deserializePlan(loadCompiledPlan());
        final String sinkTableIdentifier = "`default_catalog`.`default_database`.`B`";

        assertThat(extractTables(plan).get(sinkTableIdentifier).getResolvedTable().getOptions())
                .containsExactlyEntriesOf(ImmutableMap.of("connector", "blackhole"));

        // Apply table options overrides
        PlanOverrides.updateTableOptions(
                plan,
                ImmutableMap.of(sinkTableIdentifier, ImmutableMap.of("property-version", "2")));

        assertThat(extractTables(plan).get(sinkTableIdentifier).getResolvedTable().getOptions())
                .containsExactlyEntriesOf(
                        ImmutableMap.of(
                                "connector", "blackhole",
                                "property-version", "2"));
    }

    private Map<String, ContextResolvedTable> extractTables(InternalPlan plan) {
        ExecNodeGraph nodeGraph = ((ExecNodeGraphInternalPlan) plan).getExecNodeGraph();

        final Map<String, ContextResolvedTable> tables = new HashMap<>();
        nodeGraph.getRootNodes().forEach(node -> extractTables(node, tables));
        return tables;
    }

    private void extractTables(ExecNode<?> node, Map<String, ContextResolvedTable> tables) {
        node.getInputEdges().forEach(edge -> extractTables(edge.getSource(), tables));

        PlanOverrides.getContextResolvedTable(node)
                .ifPresent(
                        table -> {
                            final String tableIdentifier =
                                    table.getIdentifier().asSerializableString();

                            tables.put(tableIdentifier, table);
                        });
    }

    private ExecNodeGraphInternalPlan deserializePlan(String compiledPlan) throws Exception {
        PlanReference planReference = PlanReference.fromJsonString(compiledPlan);
        return (ExecNodeGraphInternalPlan) TABLE_ENVIRONMENT.getPlanner().loadPlan(planReference);
    }

    private String loadCompiledPlan() {
        try {
            return ResourceUtils.loadResource("/compiled_plan.json");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
