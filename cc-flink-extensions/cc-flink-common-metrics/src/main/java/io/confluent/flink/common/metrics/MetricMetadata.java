/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.apache.flink.annotation.Confluent;

import java.util.Map;

/**
 * Metadata associated with a given metric, stored alongside it so that it can be provided during
 * reporting time.
 */
@Confluent
public class MetricMetadata {

    static final String NAME_SEPARATOR = ".";

    private final String name;
    private final Map<String, String> variables;

    public MetricMetadata(String name, Map<String, String> variables) {
        this.name = name;
        this.variables = variables;
    }

    public MetricMetadata subMetric(String suffix) {
        return new MetricMetadata(name + NAME_SEPARATOR + suffix, variables);
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getVariables() {
        return variables;
    }
}
