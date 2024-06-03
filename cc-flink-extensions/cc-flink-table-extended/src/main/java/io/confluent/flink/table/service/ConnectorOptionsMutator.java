/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;

import java.util.Map;

/** Mutates options for connectors before putting them into {@link CompiledPlan}. */
@Confluent
public interface ConnectorOptionsMutator {

    /**
     * Enables mutating options that will be added to the {@link CompiledPlan} for the given
     * connector {@link ExecNode}.
     */
    void mutateOptions(
            ObjectIdentifier identifier, int execNodeId, Map<String, String> tableOptions);
}
