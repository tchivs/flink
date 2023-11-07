/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupTableAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalGroupWindowTableAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIncrementalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMatch;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalOverAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSink;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalSortLimit;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalValues;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowDeduplicate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowRank;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Kind of SQL operator. */
@Confluent
public enum NodeKind {
    // For new nodes that haven't been added to this list
    UNKNOWN(null),

    // StreamPhysicalRel nodes as of Flink 1.18
    CALC(StreamPhysicalCalc.class),
    CHANGELOG_NORMALIZE(StreamPhysicalChangelogNormalize.class),
    CORRELATE(StreamPhysicalCorrelate.class),
    DEDUPLICATE(StreamPhysicalDeduplicate.class),
    DROP_UPDATE_BEFORE(StreamPhysicalDropUpdateBefore.class),
    EXCHANGE(StreamPhysicalExchange.class),
    EXPAND(StreamPhysicalExpand.class),
    GLOBAL_GROUP_AGGREGATE(StreamPhysicalGlobalGroupAggregate.class),
    GLOBAL_WINDOW_AGGREGATE(StreamPhysicalGlobalWindowAggregate.class),
    GROUP_AGGREGATE(StreamPhysicalGroupAggregate.class),
    GROUP_TABLE_AGGREGATE(StreamPhysicalGroupTableAggregate.class),
    GROUP_WINDOW_AGGREGATE(StreamPhysicalGroupWindowAggregate.class),
    GROUP_WINDOW_TABLE_AGGREGATE(StreamPhysicalGroupWindowTableAggregate.class),
    INCREMENTAL_GROUP_AGGREGATE(StreamPhysicalIncrementalGroupAggregate.class),
    INTERVAL_JOIN(StreamPhysicalIntervalJoin.class),
    JOIN(StreamPhysicalJoin.class),
    LIMIT(StreamPhysicalLimit.class),
    LOCAL_GROUP_AGGREGATE(StreamPhysicalLocalGroupAggregate.class),
    LOCAL_WINDOW_AGGREGATE(StreamPhysicalLocalWindowAggregate.class),
    LOOKUP_JOIN(StreamPhysicalLookupJoin.class),
    MATCH(StreamPhysicalMatch.class),
    OVER_AGGREGATE(StreamPhysicalOverAggregate.class),
    RANK(StreamPhysicalRank.class),
    SINK(StreamPhysicalSink.class),
    SORT(StreamPhysicalSort.class),
    SORT_LIMIT(StreamPhysicalSortLimit.class),
    SOURCE_SCAN(StreamPhysicalTableSourceScan.class),
    TEMPORAL_JOIN(StreamPhysicalTemporalJoin.class),
    TEMPORAL_SORT(StreamPhysicalTemporalSort.class),
    UNION(StreamPhysicalUnion.class),
    VALUES(StreamPhysicalValues.class),
    WATERMARK_ASSIGNER(StreamPhysicalWatermarkAssigner.class),
    WINDOW_AGGREGATE(StreamPhysicalWindowAggregate.class),
    WINDOW_DEDUPLICATE(StreamPhysicalWindowDeduplicate.class),
    WINDOW_JOIN(StreamPhysicalWindowJoin.class),
    WINDOW_RANK(StreamPhysicalWindowRank.class),
    WINDOW_TABLE_FUNCTION(StreamPhysicalWindowTableFunction.class);

    public final @Nullable Class<? extends FlinkPhysicalRel> clazz;

    private static final Map<Class<? extends FlinkPhysicalRel>, NodeKind> TO_KIND =
            Stream.of(NodeKind.values())
                    .collect(Collectors.toMap(kind -> kind.clazz, kind -> kind));

    NodeKind(@Nullable Class<? extends FlinkPhysicalRel> clazz) {
        this.clazz = clazz;
    }

    // --------------------------------------------------------------------------------------------
    // Logic for NodeKind extraction
    // --------------------------------------------------------------------------------------------

    static NodeKind mapToKind(Class<? extends FlinkPhysicalRel> clazz) {
        return TO_KIND.getOrDefault(clazz, UNKNOWN);
    }
}
