/*
 * Copyright 2024 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Utility class to apply overrides to a given plan. This class is used by the {@link
 * ConfluentGeneratorUtils} to apply table options overrides to the generated plan.
 */
@Confluent
public final class PlanOverrides {
    private static final Logger LOG = LoggerFactory.getLogger(PlanOverrides.class);

    private PlanOverrides() {}

    /**
     * Applies the given table options overrides to the given plan.
     *
     * @param plan the plan to apply the overrides to
     * @param optionsOverrides the table options overrides to apply
     */
    public static void updateTableOptions(
            final ExecNodeGraphInternalPlan plan,
            final Map<String, Map<String, String>> optionsOverrides) {
        final ExecNodeGraph execNodeGraph = plan.getExecNodeGraph();
        execNodeGraph.getRootNodes().forEach(node -> updateTableOptions(node, optionsOverrides));
    }

    private static void updateTableOptions(
            final ExecNode<?> node, final Map<String, Map<String, String>> optionsOverrides) {
        node.getInputEdges()
                .forEach(edge -> updateTableOptions(edge.getSource(), optionsOverrides));

        final Optional<ContextResolvedTable> contextResolvedTable = getContextResolvedTable(node);
        if (!contextResolvedTable.isPresent()) {
            return;
        }

        final String identifier = contextResolvedTable.get().getIdentifier().asSerializableString();
        if (optionsOverrides.containsKey(identifier)) {
            LOG.info("Applying table options overrides for table '{}'", identifier);

            final Map<String, String> tableOptionsForTable = optionsOverrides.get(identifier);

            // Update the table options, which are mutable, directly in the resolved table
            contextResolvedTable.get().getResolvedTable().getOptions().putAll(tableOptionsForTable);
        }
    }

    static Optional<ContextResolvedTable> getContextResolvedTable(final ExecNode<?> node) {
        final ContextResolvedTable contextTable;
        if (node instanceof StreamExecTableSourceScan) {
            final StreamExecTableSourceScan scan = (StreamExecTableSourceScan) node;
            contextTable = scan.getTableSourceSpec().getContextResolvedTable();
        } else if (node instanceof StreamExecLookupJoin) {
            final StreamExecLookupJoin lookupJoin = (StreamExecLookupJoin) node;
            contextTable =
                    lookupJoin
                            .getTemporalTableSourceSpec()
                            .getTableSourceSpec()
                            .getContextResolvedTable();
        } else if (node instanceof StreamExecSink) {
            final StreamExecSink sink = (StreamExecSink) node;
            contextTable = sink.getTableSinkSpec().getContextResolvedTable();
        } else {
            return Optional.empty();
        }

        return Optional.of(contextTable);
    }
}
