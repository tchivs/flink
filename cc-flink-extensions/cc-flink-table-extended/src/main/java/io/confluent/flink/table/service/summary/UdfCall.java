/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.flink.shaded.guava31.com.google.common.base.Objects;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import io.confluent.flink.table.modules.remoteudf.ConfiguredRemoteScalarFunction;
import io.confluent.flink.table.modules.remoteudf.UdfUtil;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Udf call within a SQL expression. */
public class UdfCall {

    private final String path;
    private final Map<String, String> resourceConfigs;

    public UdfCall(String path, Map<String, String> resourceConfigs) {
        this.path = path;
        this.resourceConfigs = resourceConfigs;
    }

    public Map<String, String> getResourceConfigs() {
        return resourceConfigs;
    }

    public String getPath() {
        return path;
    }

    public int hashCode() {
        return Objects.hashCode(path);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        // Assume path is sufficient to uniquely define the UDF
        UdfCall that = (UdfCall) o;
        return that.path.equals(path);
    }

    public static Set<UdfCall> getUdfCalls(List<RexNode> nodes) {
        final List<ConfiguredRemoteScalarFunction> udfList = new ArrayList<>();
        nodes.forEach(rexNode -> udfList.addAll(rexNode.accept(new Extractor())));

        Set<UdfCall> udfs = new HashSet<>();
        for (ConfiguredRemoteScalarFunction function : udfList) {
            Map<String, String> config = new HashMap<>(UdfUtil.toConfiguration(function));
            udfs.add(new UdfCall(function.getPath(), config));
        }
        return udfs;
    }

    /** Extracts UDF calls from a RexNode. */
    public static class Extractor extends RexDefaultVisitor<List<ConfiguredRemoteScalarFunction>> {

        @Override
        public List<ConfiguredRemoteScalarFunction> visitNode(RexNode rexNode) {
            return ImmutableList.of();
        }

        @Override
        public List<ConfiguredRemoteScalarFunction> visitCall(RexCall call) {
            FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
            List<ConfiguredRemoteScalarFunction> result = new ArrayList<>();
            if (definition instanceof ConfiguredRemoteScalarFunction) {
                result.add((ConfiguredRemoteScalarFunction) definition);
            }
            for (RexNode node : call.getOperands()) {
                result.addAll(node.accept(this));
            }
            return result;
        }
    }
}
