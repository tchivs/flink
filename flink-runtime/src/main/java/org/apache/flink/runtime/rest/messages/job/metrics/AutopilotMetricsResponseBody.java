/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Response body of the Autopilot Metrics endpoint. */
@Confluent
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public final class AutopilotMetricsResponseBody implements ResponseBody {

    private static final String FIELD_TASK_MANAGER_METRICS = "taskManagerMetrics";
    private static final String FIELD_TASK_METRICS = "taskMetrics";
    private static final String FIELD_OPERATOR_METRICS = "operatorMetrics";

    public static AutopilotMetricsResponseBody combine(
            Collection<AutopilotMetricsResponseBody> responses) {
        final AutopilotMetricsResponseBody combined = new AutopilotMetricsResponseBody();
        for (AutopilotMetricsResponseBody toCombine : responses) {
            toCombine.taskManagerMetrics.forEach(
                    (k, v) ->
                            combined.taskManagerMetrics.merge(
                                    k, v, (oldValue, newValue) -> newValue));
            toCombine.taskMetrics.forEach(
                    (k, v) -> combined.taskMetrics.merge(k, v, (oldValue, newValue) -> newValue));
            toCombine.operatorMetrics.forEach(
                    (k, v) ->
                            combined.operatorMetrics.merge(k, v, (oldValue, newValue) -> newValue));
        }
        return combined;
    }

    @JsonProperty(FIELD_TASK_MANAGER_METRICS)
    private final Map<String, Map<String, Number>> taskManagerMetrics;

    @JsonProperty(FIELD_TASK_METRICS)
    private final Map<String, Map<String, Number>> taskMetrics;

    @JsonProperty(FIELD_OPERATOR_METRICS)
    private final Map<String, Map<String, Number>> operatorMetrics;

    @JsonCreator
    @VisibleForTesting
    AutopilotMetricsResponseBody(
            @JsonProperty(FIELD_TASK_MANAGER_METRICS) @Nullable
                    Map<String, Map<String, Number>> taskManagerMetrics,
            @Nullable @JsonProperty(FIELD_TASK_METRICS)
                    Map<String, Map<String, Number>> taskMetrics,
            @Nullable @JsonProperty(FIELD_OPERATOR_METRICS)
                    Map<String, Map<String, Number>> operatorMetrics) {
        this.taskManagerMetrics = taskManagerMetrics != null ? taskManagerMetrics : new HashMap<>();
        this.taskMetrics = taskMetrics != null ? taskMetrics : new HashMap<>();
        this.operatorMetrics = operatorMetrics != null ? operatorMetrics : new HashMap<>();
    }

    public AutopilotMetricsResponseBody() {
        this(new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    public void addMetric(MetricDump metricDump) throws ParseException {
        switch (metricDump.scopeInfo.getCategory()) {
            case QueryScopeInfo.INFO_CATEGORY_TM:
                {
                    final QueryScopeInfo.TaskManagerQueryScopeInfo cast =
                            (QueryScopeInfo.TaskManagerQueryScopeInfo) metricDump.scopeInfo;
                    addMetric(
                            taskManagerMetrics.computeIfAbsent(
                                    cast.taskManagerID, ignored -> new HashMap<>()),
                            metricDump);
                    break;
                }
            case QueryScopeInfo.INFO_CATEGORY_TASK:
                {
                    final QueryScopeInfo.TaskQueryScopeInfo cast =
                            (QueryScopeInfo.TaskQueryScopeInfo) metricDump.scopeInfo;
                    addMetric(
                            taskMetrics.computeIfAbsent(
                                    String.format(
                                            "%s-%d-%d",
                                            cast.vertexID, cast.subtaskIndex, cast.attemptNumber),
                                    ignored -> new HashMap<>()),
                            metricDump);
                    break;
                }
            case QueryScopeInfo.INFO_CATEGORY_OPERATOR:
                {
                    final QueryScopeInfo.OperatorQueryScopeInfo cast =
                            (QueryScopeInfo.OperatorQueryScopeInfo) metricDump.scopeInfo;
                    addMetric(
                            operatorMetrics.computeIfAbsent(
                                    String.format(
                                            "%s@%s-%d-%d",
                                            cast.operatorId,
                                            cast.vertexID,
                                            cast.subtaskIndex,
                                            cast.attemptNumber),
                                    ignored -> new HashMap<>()),
                            metricDump);
                    break;
                }
        }
    }

    private static void addMetric(Map<String, Number> metrics, MetricDump metricDump)
            throws ParseException {
        final String scope = metricDump.scopeInfo.scope;
        final String fullName = scope.isEmpty() ? metricDump.name : scope + "." + metricDump.name;
        switch (metricDump.getCategory()) {
            case MetricDump.METRIC_CATEGORY_COUNTER:
                final MetricDump.CounterDump counterDump = (MetricDump.CounterDump) metricDump;
                metrics.put(fullName, counterDump.count);
                break;
            case MetricDump.METRIC_CATEGORY_GAUGE:
                final MetricDump.GaugeDump gaugeDump = (MetricDump.GaugeDump) metricDump;
                metrics.put(fullName, NumberFormat.getInstance().parse(gaugeDump.value));
                break;
            case MetricDump.METRIC_CATEGORY_METER:
                final MetricDump.MeterDump meterDump = (MetricDump.MeterDump) metricDump;
                metrics.put(fullName, meterDump.rate);
                break;
        }
    }

    public Map<String, Map<String, Number>> getTaskManagerMetrics() {
        return taskManagerMetrics;
    }

    public Map<String, Map<String, Number>> getTaskMetrics() {
        return taskMetrics;
    }

    public Map<String, Map<String, Number>> getOperatorMetrics() {
        return operatorMetrics;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AutopilotMetricsResponseBody that = (AutopilotMetricsResponseBody) o;
        return Objects.equals(taskManagerMetrics, that.taskManagerMetrics)
                && Objects.equals(taskMetrics, that.taskMetrics)
                && Objects.equals(operatorMetrics, that.operatorMetrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskManagerMetrics, taskMetrics, operatorMetrics);
    }

    @Override
    public String toString() {
        return "AutopilotMetricsResponseBody{"
                + "taskManagerMetrics="
                + taskManagerMetrics
                + ", taskMetrics="
                + taskMetrics
                + ", operatorMetrics="
                + operatorMetrics
                + '}';
    }
}
