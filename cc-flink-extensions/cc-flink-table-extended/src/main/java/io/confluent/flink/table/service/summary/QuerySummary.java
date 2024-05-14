/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Summary of the physical query plan. Generated from a graph of {@link FlinkPhysicalRel}.
 *
 * <p>This class is an abstracted view of {@link FlinkPhysicalRel}s. It enables insights without
 * digging into Calcite-specific classes and focuses on the important pieces. It allows for
 * simplified static query analytics while not affecting planning latency.
 *
 * <p>This class is intended for plan checks, query metrics, and more in the future. Instead of
 * iterating through Flink's complex class topology, this class can be extended for Confluent's
 * needs.
 */
@Confluent
public class QuerySummary {

    private final Set<QueryProperty> queryProperties = EnumSet.noneOf(QueryProperty.class);
    private final List<NodeSummary> nodeSummaries = new ArrayList<>();
    private int[] foregroundSinkUpsertKeys = new int[0];

    // Derived from nodeSummaries and cached
    private Set<NodeKind> nodeKinds;
    private Set<ExpressionKind> expressionKinds;
    private Set<UdfCall> udfCalls;

    public Set<QueryProperty> getProperties() {
        return queryProperties;
    }

    public boolean hasProperty(QueryProperty property) {
        return queryProperties.contains(property);
    }

    public List<NodeSummary> getNodes() {
        return nodeSummaries;
    }

    public Set<NodeKind> getNodeKinds() {
        if (nodeKinds == null) {
            nodeKinds = EnumSet.noneOf(NodeKind.class);
            nodeSummaries.forEach(n -> collectNodeKinds(nodeKinds, n));
        }
        return nodeKinds;
    }

    public Set<ExpressionKind> getExpressionKinds() {
        if (expressionKinds == null) {
            expressionKinds = EnumSet.noneOf(ExpressionKind.class);
            nodeSummaries.forEach(n -> collectExpressionKinds(expressionKinds, n));
        }
        return expressionKinds;
    }

    public Set<UdfCall> getUdfCalls() {
        if (udfCalls == null) {
            udfCalls = new HashSet<>();
            nodeSummaries.forEach(n -> collectUdfCalls(udfCalls, n));
        }
        return udfCalls;
    }

    public <T> List<T> getNodeTagValues(NodeTag tag, Class<T> type) {
        final List<T> tagValues = new ArrayList<>();
        nodeSummaries.forEach(n -> collectNodeTagValues(tagValues, n, tag, type));
        return tagValues;
    }

    public int[] getForegroundSinkUpsertKeys() {
        return foregroundSinkUpsertKeys;
    }

    public void ingestPhysicalGraph(boolean isForeground, List<FlinkPhysicalRel> physicalGraph) {
        physicalGraph.stream().map(QuerySummary::summarizeNode).forEach(nodeSummaries::add);

        // Foreground / background sinks
        if (isForeground) {
            queryProperties.add(QueryProperty.FOREGROUND);
            queryProperties.add(QueryProperty.SINGLE_SINK);
        } else {
            queryProperties.add(QueryProperty.BACKGROUND);
            if (physicalGraph.size() == 1) {
                queryProperties.add(QueryProperty.SINGLE_SINK);
            } else {
                queryProperties.add(QueryProperty.MULTI_SINK);
            }
        }

        // Bounded / unbounded
        final boolean atLeastOneUnbounded =
                getNodeTagValues(NodeTag.BOUNDED, Boolean.class).contains(Boolean.FALSE);
        if (atLeastOneUnbounded) {
            queryProperties.add(QueryProperty.UNBOUNDED);
        } else {
            queryProperties.add(QueryProperty.BOUNDED);
        }
    }

    public void ingestExecNodeGraph(ExecNodeGraph execNodeGraph) {
        boolean pointInTime =
                execNodeGraph.getRootNodes().stream().anyMatch(n -> n instanceof BatchExecSink);
        if (pointInTime) {
            queryProperties.add(QueryProperty.APPEND_ONLY);
            return;
        }

        // Append-only / updating sink inputs
        final boolean isAppendOnly =
                execNodeGraph.getRootNodes().stream()
                        .map(StreamExecSink.class::cast)
                        .map(StreamExecSink::getInputChangelogMode)
                        .allMatch(mode -> mode.containsOnly(RowKind.INSERT));
        if (isAppendOnly) {
            queryProperties.add(QueryProperty.APPEND_ONLY);
        } else {
            queryProperties.add(QueryProperty.UPDATING);
        }

        // Upsert keys
        if (queryProperties.contains(QueryProperty.FOREGROUND)) {
            final StreamExecSink foregroundSink =
                    (StreamExecSink) execNodeGraph.getRootNodes().get(0);
            foregroundSinkUpsertKeys = foregroundSink.getInputUpsertKey();
        }
    }

    private static void collectNodeKinds(Set<NodeKind> set, NodeSummary node) {
        node.getInputs().forEach(i -> collectNodeKinds(set, i));
        set.add(node.getKind());
    }

    @SuppressWarnings("unchecked")
    private static void collectExpressionKinds(Set<ExpressionKind> set, NodeSummary node) {
        node.getInputs().forEach(i -> collectExpressionKinds(set, i));
        final Set<ExpressionKind> expressions = node.getTag(NodeTag.EXPRESSIONS, Set.class);
        if (expressions != null) {
            set.addAll(expressions);
        }
    }

    @SuppressWarnings("unchecked")
    private static void collectUdfCalls(Set<UdfCall> set, NodeSummary node) {
        node.getInputs().forEach(i -> collectUdfCalls(set, i));
        final Set<UdfCall> expressions = node.getTag(NodeTag.UDF_CALLS, Set.class);
        if (expressions != null) {
            set.addAll(expressions);
        }
    }

    private static <T> void collectNodeTagValues(
            List<T> tagValues, NodeSummary node, NodeTag tag, Class<T> type) {
        node.getInputs().forEach(i -> collectNodeTagValues(tagValues, i, tag, type));
        final T value = node.getTag(tag, type);
        if (value != null) {
            tagValues.add(value);
        }
    }

    private static NodeSummary summarizeNode(FlinkPhysicalRel physicalRel) {
        final NodeKind kind = NodeKind.mapToKind(physicalRel.getClass());

        final List<NodeSummary> inputs =
                physicalRel.getInputs().stream()
                        .map(input -> summarizeNode((FlinkPhysicalRel) input))
                        .collect(Collectors.toList());

        final Map<NodeTag, Object> tags = new HashMap<>();
        final List<NodeTag.Extractor> extractors = NodeTag.EXTRACTION.get(kind);
        if (extractors != null && !extractors.isEmpty()) {
            extractors.forEach(e -> e.extract(physicalRel, tags));
        }

        return new NodeSummary(kind, inputs, tags);
    }
}
