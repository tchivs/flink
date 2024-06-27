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

import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.events.Event;
import org.apache.flink.events.EventBuilder;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.enable;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link AllSubTasksRunningOrFinishedStateTimeMetrics}. */
class AllSubTasksRunningOrFinishedStateTimeMetricsTest {

    private static final MetricOptions.JobStatusMetricsSettings settings =
            enable(
                    MetricOptions.JobStatusMetrics.STATE,
                    MetricOptions.JobStatusMetrics.CURRENT_TIME,
                    MetricOptions.JobStatusMetrics.TOTAL_TIME);

    @Test
    void testInitialValues() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final AllSubTasksRunningOrFinishedStateTimeMetrics runningMetrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(
                        JobType.STREAMING, settings, clock);

        assertThat(runningMetrics.getCurrentTime()).isEqualTo(0L);
        assertThat(runningMetrics.getTotalTime()).isEqualTo(0L);
        assertThat(runningMetrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testScheduledToFinishedWithSingleExecutionNoFailure() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        final ExecutionAttemptID id1 = createExecutionAttemptId();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testScheduledToFinishedWithSingleExecutionWithFailure() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        final JobVertexID jobVertexID = new JobVertexID();
        final ExecutionVertexID executionVertexID = new ExecutionVertexID(jobVertexID, 0);

        final ExecutionAttemptID id1 = createExecutionAttemptId(executionVertexID, 0);

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        final ExecutionAttemptID id2 = createExecutionAttemptId(executionVertexID, 1);

        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testScheduledToFinishedWithMultipleExecutionsNoFailure() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        final JobVertexID jobVertexID = new JobVertexID();

        final ExecutionAttemptID id1 =
                createExecutionAttemptId(new ExecutionVertexID(jobVertexID, 0), 0);
        final ExecutionAttemptID id2 =
                createExecutionAttemptId(new ExecutionVertexID(jobVertexID, 1), 0);

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        // Only signal running once both executions are in running state
        metrics.onStateUpdate(id2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        // Maintain value 1 as executions finish
        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testScheduledToFinishedWithMultipleExecutionsWithFailure() {
        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(JobType.STREAMING, settings);

        final JobVertexID jobVertexID = new JobVertexID();
        ExecutionVertexID executionVertexIDSubtask0 = new ExecutionVertexID(jobVertexID, 0);

        final ExecutionAttemptID id1 = createExecutionAttemptId(executionVertexIDSubtask0, 0);
        final ExecutionAttemptID id2 =
                createExecutionAttemptId(new ExecutionVertexID(jobVertexID, 1), 0);

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        // Only signal running once both executions are in running state
        metrics.onStateUpdate(id2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        // One subtask cancelling move the state to 0
        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.CANCELED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        final ExecutionAttemptID id1V2 = createExecutionAttemptId(executionVertexIDSubtask0, 1);

        metrics.onStateUpdate(id1V2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1V2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1V2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        // Both subtasks again up and running
        metrics.onStateUpdate(id1V2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id1V2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testGetCurrentTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(
                        JobType.STREAMING, settings, clock);

        final JobVertexID jobVertexID = new JobVertexID();
        final ExecutionVertexID executionVertexID = new ExecutionVertexID(jobVertexID, 0);

        final ExecutionAttemptID id1 = createExecutionAttemptId(executionVertexID, 0);

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(5L);
        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metrics.getCurrentTime()).isEqualTo(15L);
        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(id1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);

        final ExecutionAttemptID id2 = createExecutionAttemptId(executionVertexID, 1);
        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
        metrics.onStateUpdate(id2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(5L);
        metrics.onStateUpdate(id2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metrics.getCurrentTime()).isEqualTo(15L);
    }

    @Test
    void testGetTotalTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(
                        JobType.STREAMING, settings, clock);

        final JobVertexID jobVertexID = new JobVertexID();
        final ExecutionVertexID executionVertexID = new ExecutionVertexID(jobVertexID, 0);

        final ExecutionAttemptID id1 = createExecutionAttemptId(executionVertexID, 0);

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(0L);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(0L);
        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(0L);
        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(5L);
        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(id1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);

        final ExecutionAttemptID id2 = createExecutionAttemptId(executionVertexID, 1);
        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(15L);
        metrics.onStateUpdate(id2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(20L);
        metrics.onStateUpdate(id2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        clock.advanceTime(Duration.ofMillis(10));
        assertThat(metrics.getTotalTime()).isEqualTo(30L);
    }

    @Test
    void testStatusEvents() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final AllSubTasksRunningOrFinishedStateTimeMetrics metrics =
                new AllSubTasksRunningOrFinishedStateTimeMetrics(
                        JobType.STREAMING, settings, clock);

        final List<Event> events = new ArrayList<>();

        metrics.registerMetrics(
                new UnregisteredMetricsGroup() {
                    @Override
                    public void addEvent(EventBuilder eventBuilder) {
                        events.add(eventBuilder.build(getAllVariables()));
                    }
                });

        final JobVertexID jobVertexID = new JobVertexID();
        final ExecutionVertexID executionVertexID = new ExecutionVertexID(jobVertexID, 0);

        final ExecutionAttemptID id1 = createExecutionAttemptId(executionVertexID, 0);

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(events).hasSize(1);
        assertThat(events.remove(0).getAttributes()).containsEntry("allRunningOrFinished", "true");
        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        assertThat(events).hasSize(1);
        assertThat(events.remove(0).getAttributes()).containsEntry("allRunningOrFinished", "false");
        metrics.onStateUpdate(id1, ExecutionState.CANCELING, ExecutionState.CANCELED);
        assertThat(events).isEmpty();

        final ExecutionAttemptID id2 = createExecutionAttemptId(executionVertexID, 1);
        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(events).isEmpty();
        metrics.onStateUpdate(id2, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        assertThat(events).hasSize(1);
        assertThat(events.remove(0).getAttributes()).containsEntry("allRunningOrFinished", "true");
        metrics.onStateUpdate(id2, ExecutionState.RUNNING, ExecutionState.FINISHED);
        assertThat(events).isEmpty();
    }
}
