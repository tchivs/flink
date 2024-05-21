/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.local;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;

import io.confluent.flink.table.service.summary.QuerySummary;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Searches for patterns in the given graph to figure out whether the given query can be executed
 * locally without requiring a cluster (i.e. for simple INFORMATION_SCHEMA queries).
 *
 * <p>New rules can be added to the list of {@link #RULES}.
 */
@Confluent
public interface LocalExecution {

    List<LocalExecution> RULES = Collections.singletonList(InfoSchemaExecution.INSTANCE);

    /**
     * Searches for patterns that qualify for a local execution.
     *
     * <p>This method should be implemented with latency considerations in mind. It is called for
     * every SQL statement.
     *
     * <p>It is a necessary condition before calling {@link #execute(SerdeContext, List)}, but not
     * sufficient as {@link #execute(SerdeContext, List)} can still reject the query.
     */
    boolean matches(QuerySummary summary);

    /**
     * Tries to execute the given SQL statement locally if and only if the {@link
     * #matches(QuerySummary)} returned true.
     *
     * <p>Local execution is best-effort. If a local execution is not possible, {@link
     * Optional#empty()} is returned and a regular cluster execution should be performed.
     */
    Optional<Stream<RowData>> execute(
            SerdeContext serdeContext, List<FlinkPhysicalRel> physicalGraph);
}
