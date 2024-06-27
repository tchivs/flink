/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.events.Events;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

/**
 * Metrics and event that captures how long a job has all it's subtask in running or finished state.
 *
 * <p>This is only interesting for streaming jobs, and we don't register for batch.
 */
public class AllSubTasksRunningOrFinishedStateTimeMetrics
        implements ExecutionStatusMetricsRegistrar, StateTimeMetric {

    private static final long NOT_STARTED = -1L;
    private final MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings;
    private final Clock clock;

    /**
     * This set keeps track of all executions for which we require to observe a transition to
     * running or finished.
     */
    private final Set<ExecutionVertexID> notRunningOrFinishedVertices = new HashSet<>();

    /** Tracks how long we have been in the all running/finished state in the current epoch. */
    private long currentAllRunningOrFinishedStartTime = NOT_STARTED;

    /**
     * Tracks how long we have been in the all running/finished state in the all previous epochs.
     */
    private long allRunningOrFinishedAccumulatedTime = 0L;

    @Nullable private MetricGroup registeredMetricGroup;

    public AllSubTasksRunningOrFinishedStateTimeMetrics(
            JobType semantic, MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings) {
        this(semantic, stateTimeMetricsSettings, SystemClock.getInstance());
    }

    @VisibleForTesting
    AllSubTasksRunningOrFinishedStateTimeMetrics(
            JobType jobType,
            MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings,
            Clock clock) {
        Preconditions.checkState(
                jobType == JobType.STREAMING,
                "This metric should only be created and registered for streaming jobs!");
        this.stateTimeMetricsSettings = stateTimeMetricsSettings;
        this.clock = clock;
        this.registeredMetricGroup = null;
    }

    @Override
    public long getCurrentTime() {
        return currentAllRunningOrFinishedStartTime == NOT_STARTED
                ? 0L
                : Math.max(0, clock.absoluteTimeMillis() - currentAllRunningOrFinishedStartTime);
    }

    @Override
    public long getTotalTime() {
        return getCurrentTime() + allRunningOrFinishedAccumulatedTime;
    }

    @Override
    public long getBinary() {
        return currentAllRunningOrFinishedStartTime == NOT_STARTED ? 0L : 1L;
    }

    @Override
    public void registerMetrics(MetricGroup metricGroup) {
        StateTimeMetric.register(
                stateTimeMetricsSettings, metricGroup, this, "allSubTasksRunningOrFinished");
        // Remember the metric group to which we registered so that events can be reported to it.
        this.registeredMetricGroup = metricGroup;
    }

    @Override
    public void onStateUpdate(
            ExecutionAttemptID execution, ExecutionState previousState, ExecutionState newState) {

        ExecutionVertexID executionVertexId = execution.getExecutionVertexId();
        if (newState == ExecutionState.RUNNING || newState == ExecutionState.FINISHED) {
            notRunningOrFinishedVertices.remove(executionVertexId);
        } else {
            notRunningOrFinishedVertices.add(executionVertexId);
        }

        if (notRunningOrFinishedVertices.isEmpty()
                && currentAllRunningOrFinishedStartTime == NOT_STARTED) {
            // Transitioning "all running/finished" from false to true
            currentAllRunningOrFinishedStartTime = clock.absoluteTimeMillis();
            reportAllSubtaskStatusChangeEvent(true);
        } else if (!notRunningOrFinishedVertices.isEmpty()
                && currentAllRunningOrFinishedStartTime != NOT_STARTED) {
            // Transitioning "all running/finished" from true to false
            allRunningOrFinishedAccumulatedTime +=
                    Math.max(0, clock.absoluteTimeMillis() - currentAllRunningOrFinishedStartTime);
            currentAllRunningOrFinishedStartTime = NOT_STARTED;
            reportAllSubtaskStatusChangeEvent(false);
        }
    }

    private void reportAllSubtaskStatusChangeEvent(boolean allRunningOrFinished) {
        final MetricGroup metricGroup = this.registeredMetricGroup;
        if (metricGroup != null) {
            final String bodyText =
                    allRunningOrFinished
                            ? "All subtasks are now in running or finished state."
                            : "Not all subtasks are in running or finished state anymore.";
            metricGroup.addEvent(
                    Events.AllSubtasksStatusChange.builder(
                                    AllSubTasksRunningOrFinishedStateTimeMetrics.class)
                            .setSeverity("INFO")
                            .setObservedTsMillis(clock.absoluteTimeMillis())
                            .setBody(bodyText)
                            .setAttribute(
                                    "allRunningOrFinished", String.valueOf(allRunningOrFinished)));
        }
    }
}
