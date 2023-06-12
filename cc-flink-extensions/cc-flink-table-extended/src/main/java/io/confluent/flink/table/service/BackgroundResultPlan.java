/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;

import java.util.List;

/** {@link CompiledPlan} for a background job. */
@Confluent
public class BackgroundResultPlan {

    private final String compiledPlan;

    private final List<ConnectorMetadata> connectorMetadata;

    public BackgroundResultPlan(String compiledPlan, List<ConnectorMetadata> connectorMetadata) {
        this.compiledPlan = compiledPlan;
        this.connectorMetadata = connectorMetadata;
    }

    public String getCompiledPlan() {
        return compiledPlan;
    }

    public List<ConnectorMetadata> getConnectorMetadata() {
        return connectorMetadata;
    }
}
