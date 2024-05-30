/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalAsyncCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import org.apache.calcite.rel.core.Join;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Typed tags that can be attached to a {@link NodeSummary}. */
@Confluent
public enum NodeTag {
    /** Identifier of a source or sink table. */
    TABLE_IDENTIFIER(ObjectIdentifier.class),

    /** {@link ExpressionKind}s of projections, filters, etc. */
    EXPRESSIONS(Set.class),

    /** {@link DynamicTableSourceFactory#factoryIdentifier()}. */
    SOURCE_CONNECTOR(String.class),

    /** {@link ScanTableSource.ScanRuntimeProvider#isBounded()}. */
    BOUNDED(Boolean.class),

    /** {@link UdfCall}s within the SQL expressions. */
    UDF_CALLS(Set.class);

    private final Class<?> type;

    NodeTag(Class<?> type) {
        this.type = type;
    }

    public Class<?> getType() {
        return type;
    }

    // --------------------------------------------------------------------------------------------
    // Logic for NodeTag extraction
    // --------------------------------------------------------------------------------------------

    interface Extractor {
        void extract(FlinkPhysicalRel rel, Map<NodeTag, Object> tags);
    }

    static final Map<NodeKind, List<Extractor>> EXTRACTION = initExtraction();

    private static Map<NodeKind, List<Extractor>> initExtraction() {
        final Map<NodeKind, List<Extractor>> map = new HashMap<>();

        addNodeTagExtraction(
                map,
                NodeKind.CALC,
                (rel, tags) -> {
                    final StreamPhysicalCalc calc = (StreamPhysicalCalc) rel;
                    final ExpressionKind.Extractor extractor = new ExpressionKind.Extractor();
                    calc.getProgram().getExprList().forEach(e -> e.accept(extractor));
                    tags.put(NodeTag.EXPRESSIONS, extractor.kinds);

                    // Extract UDF metadata
                    tags.put(
                            NodeTag.UDF_CALLS,
                            UdfCall.getUdfCalls(calc.getProgram().getExprList()));
                });

        addNodeTagExtraction(
                map,
                NodeKind.ASYNC_CALC,
                (rel, tags) -> {
                    final StreamPhysicalAsyncCalc asyncCalc = (StreamPhysicalAsyncCalc) rel;
                    final ExpressionKind.Extractor extractor = new ExpressionKind.Extractor();
                    asyncCalc.getProgram().getExprList().forEach(e -> e.accept(extractor));
                    tags.put(NodeTag.EXPRESSIONS, extractor.kinds);

                    // Extract UDF metadata
                    tags.put(
                            NodeTag.UDF_CALLS,
                            UdfCall.getUdfCalls(asyncCalc.getProgram().getExprList()));
                });

        addNodeTagExtraction(
                map,
                NodeKind.SOURCE_SCAN,
                (rel, tags) -> {
                    final StreamPhysicalTableSourceScan scan = (StreamPhysicalTableSourceScan) rel;
                    final TableSourceTable table = scan.tableSourceTable();
                    final ContextResolvedTable contextTable = table.contextResolvedTable();
                    final ScanTableSource scanTableSource = (ScanTableSource) table.tableSource();

                    tags.put(NodeTag.TABLE_IDENTIFIER, contextTable.getIdentifier());
                    tags.put(
                            NodeTag.SOURCE_CONNECTOR,
                            contextTable.getTable().getOptions().get(FactoryUtil.CONNECTOR.key()));
                    tags.put(
                            NodeTag.BOUNDED,
                            scanTableSource
                                    .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE)
                                    .isBounded());
                });

        addNodeTagExtraction(
                map,
                NodeKind.SINK,
                (rel, tags) -> {
                    final StreamPhysicalSink sink = (StreamPhysicalSink) rel;
                    final ContextResolvedTable contextTable = sink.contextResolvedTable();

                    tags.put(NodeTag.TABLE_IDENTIFIER, contextTable.getIdentifier());
                });

        extractJoins(
                map, new NodeKind[] {NodeKind.JOIN, NodeKind.TEMPORAL_JOIN, NodeKind.WINDOW_JOIN});

        return map;
    }

    private static void extractJoins(Map<NodeKind, List<Extractor>> map, NodeKind[] kinds) {
        for (NodeKind nodeKind : kinds) {
            addNodeTagExtraction(
                    map,
                    nodeKind,
                    (rel, tags) -> {
                        final Join join = (Join) rel;

                        // Extract UDF metadata
                        tags.put(
                                NodeTag.UDF_CALLS,
                                UdfCall.getUdfCalls(
                                        Collections.singletonList(join.getCondition())));
                    });
        }
    }

    private static void addNodeTagExtraction(
            Map<NodeKind, List<Extractor>> map, NodeKind kind, Extractor extractor) {
        final List<Extractor> extractors = map.computeIfAbsent(kind, k -> new ArrayList<>());
        extractors.add(extractor);
    }
}
