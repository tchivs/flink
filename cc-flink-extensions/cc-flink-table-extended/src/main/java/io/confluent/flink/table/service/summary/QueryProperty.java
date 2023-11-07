/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Global characteristics of a query. */
@Confluent
public enum QueryProperty {
    SINGLE_SINK,
    MULTI_SINK,
    FOREGROUND,
    BACKGROUND;

    // --------------------------------------------------------------------------------------------
    // Logic for QueryProperty extraction
    // --------------------------------------------------------------------------------------------

    static Set<QueryProperty> extract(boolean isForeground, List<FlinkPhysicalRel> physicalGraph) {
        final Set<QueryProperty> queryProperties = EnumSet.noneOf(QueryProperty.class);

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

        return queryProperties;
    }
}
