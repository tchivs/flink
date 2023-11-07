/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Summary of a {@link FlinkPhysicalRel}. */
@Confluent
public class NodeSummary {
    private final NodeKind kind;
    private final List<NodeSummary> inputs;

    private final Map<NodeTag, Object> tags;

    public NodeSummary(NodeKind kind, List<NodeSummary> inputs, Map<NodeTag, Object> tags) {
        this.kind = kind;
        this.inputs = inputs;
        this.tags = tags;
    }

    public NodeKind getKind() {
        return kind;
    }

    public List<NodeSummary> getInputs() {
        return inputs;
    }

    @SuppressWarnings("unchecked")
    public <T> @Nullable T getTag(NodeTag tag, Class<T> type) {
        final Object value = tags.get(tag);
        if (value == null) {
            return null;
        }
        if (!type.isAssignableFrom(value.getClass())) {
            throw new IllegalStateException("Invalid tag defined.");
        }

        return (T) value;
    }
}
