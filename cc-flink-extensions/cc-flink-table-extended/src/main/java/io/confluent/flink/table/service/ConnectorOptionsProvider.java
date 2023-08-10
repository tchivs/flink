/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;

import java.util.Map;

/**
 * Generates private options for connectors that must be globally unique in the {@link
 * CompiledPlan}.
 */
@Confluent
public interface ConnectorOptionsProvider {

    /**
     * Returns a map of options that will be added to the {@link CompiledPlan} for the given
     * connector {@link ExecNode}.
     */
    Map<String, String> generateOptions(ObjectIdentifier identifier, int execNodeId);
}
