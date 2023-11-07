/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
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

    private final Set<QueryProperty> queryProperties;
    private final List<NodeSummary> nodeSummaries;

    // Derived from nodeSummaries and cached
    private Set<NodeKind> nodeKinds;
    private Set<ExpressionKind> expressionKinds;

    private QuerySummary(Set<QueryProperty> queryProperties, List<NodeSummary> nodeSummaries) {
        this.queryProperties = queryProperties;
        this.nodeSummaries = nodeSummaries;
    }

    public static QuerySummary summarize(
            boolean isForeground, List<FlinkPhysicalRel> physicalGraph) {
        final Set<QueryProperty> queryProperties =
                QueryProperty.extract(isForeground, physicalGraph);

        final List<NodeSummary> nodeSummaries =
                physicalGraph.stream()
                        .map(QuerySummary::summarizeNode)
                        .collect(Collectors.toList());

        return new QuerySummary(queryProperties, nodeSummaries);
    }

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

    public <T> List<T> getNodeTagValues(NodeTag tag, Class<T> type) {
        final List<T> tagValues = new ArrayList<>();
        nodeSummaries.forEach(n -> collectNodeTagValues(tagValues, n, tag, type));
        return tagValues;
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
