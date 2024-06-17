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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.events.Event;
import org.apache.flink.events.EventBuilder;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CheckpointStatsTrackerTest {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    /** Tests that the number of remembered checkpoints configuration is respected. */
    @Test
    public void testTrackerWithoutHistory() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, 3, 256)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex = graph.getJobVertex(jobVertexID);

        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(
                        0,
                        new UnregisteredMetricsGroup(),
                        new JobID(),
                        TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT_WITH_TASKS,
                        true);

        PendingCheckpointStats pending =
                tracker.reportPendingCheckpoint(
                        0,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, jobVertex.getParallelism()));

        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(0));
        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(1));
        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(2));

        tracker.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(null));

        CheckpointStatsSnapshot snapshot = tracker.createSnapshot();
        // History should be empty
        assertFalse(snapshot.getHistory().getCheckpoints().iterator().hasNext());

        // Counts should be available
        CheckpointStatsCounts counts = snapshot.getCounts();
        assertEquals(1, counts.getNumberOfCompletedCheckpoints());
        assertEquals(1, counts.getTotalNumberOfCheckpoints());

        // Summary should be available
        CompletedCheckpointStatsSummarySnapshot summary = snapshot.getSummaryStats();
        assertEquals(1, summary.getStateSizeStats().getCount());
        assertEquals(1, summary.getEndToEndDurationStats().getCount());

        // Latest completed checkpoint
        assertNotNull(snapshot.getHistory().getLatestCompletedCheckpoint());
        assertEquals(0, snapshot.getHistory().getLatestCompletedCheckpoint().getCheckpointId());
    }

    /** Tests tracking of checkpoints. */
    @Test
    public void testCheckpointTracking() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID, 3, 256)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex = graph.getJobVertex(jobVertexID);
        Map<JobVertexID, Integer> vertexToDop =
                singletonMap(jobVertexID, jobVertex.getParallelism());

        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(
                        10,
                        new UnregisteredMetricsGroup(),
                        new JobID(),
                        TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT_WITH_TASKS,
                        true);

        // Completed checkpoint
        PendingCheckpointStats completed1 =
                tracker.reportPendingCheckpoint(
                        0,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        vertexToDop);

        completed1.reportSubtaskStats(jobVertexID, createSubtaskStats(0));
        completed1.reportSubtaskStats(jobVertexID, createSubtaskStats(1));
        completed1.reportSubtaskStats(jobVertexID, createSubtaskStats(2));

        tracker.reportCompletedCheckpoint(completed1.toCompletedCheckpointStats(null));

        // Failed checkpoint
        PendingCheckpointStats failed =
                tracker.reportPendingCheckpoint(
                        1,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        vertexToDop);

        tracker.reportFailedCheckpoint(failed.toFailedCheckpoint(12, null));

        // Completed savepoint
        PendingCheckpointStats savepoint =
                tracker.reportPendingCheckpoint(
                        2,
                        1,
                        CheckpointProperties.forSavepoint(true, SavepointFormatType.CANONICAL),
                        vertexToDop);

        savepoint.reportSubtaskStats(jobVertexID, createSubtaskStats(0));
        savepoint.reportSubtaskStats(jobVertexID, createSubtaskStats(1));
        savepoint.reportSubtaskStats(jobVertexID, createSubtaskStats(2));

        tracker.reportCompletedCheckpoint(savepoint.toCompletedCheckpointStats(null));

        // In Progress
        PendingCheckpointStats inProgress =
                tracker.reportPendingCheckpoint(
                        3,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        vertexToDop);

        RestoredCheckpointStats restored =
                new RestoredCheckpointStats(
                        81,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        123,
                        null,
                        42);
        tracker.reportInitializationStartTs(123);
        tracker.reportRestoredCheckpoint(restored);

        CheckpointStatsSnapshot snapshot = tracker.createSnapshot();

        // Counts
        CheckpointStatsCounts counts = snapshot.getCounts();
        assertEquals(4, counts.getTotalNumberOfCheckpoints());
        assertEquals(1, counts.getNumberOfInProgressCheckpoints());
        assertEquals(2, counts.getNumberOfCompletedCheckpoints());
        assertEquals(1, counts.getNumberOfFailedCheckpoints());

        tracker.reportFailedCheckpointsWithoutInProgress();

        CheckpointStatsSnapshot snapshot1 = tracker.createSnapshot();
        counts = snapshot1.getCounts();
        assertEquals(5, counts.getTotalNumberOfCheckpoints());
        assertEquals(1, counts.getNumberOfInProgressCheckpoints());
        assertEquals(2, counts.getNumberOfCompletedCheckpoints());
        assertEquals(2, counts.getNumberOfFailedCheckpoints());

        // Summary stats
        CompletedCheckpointStatsSummarySnapshot summary = snapshot.getSummaryStats();
        assertEquals(2, summary.getStateSizeStats().getCount());
        assertEquals(2, summary.getEndToEndDurationStats().getCount());

        // History
        CheckpointStatsHistory history = snapshot.getHistory();
        Iterator<AbstractCheckpointStats> it = history.getCheckpoints().iterator();

        assertTrue(it.hasNext());
        AbstractCheckpointStats stats = it.next();
        assertEquals(3, stats.getCheckpointId());
        assertTrue(stats.getStatus().isInProgress());

        assertTrue(it.hasNext());
        stats = it.next();
        assertEquals(2, stats.getCheckpointId());
        assertTrue(stats.getStatus().isCompleted());

        assertTrue(it.hasNext());
        stats = it.next();
        assertEquals(1, stats.getCheckpointId());
        assertTrue(stats.getStatus().isFailed());

        assertTrue(it.hasNext());
        stats = it.next();
        assertEquals(0, stats.getCheckpointId());
        assertTrue(stats.getStatus().isCompleted());

        assertFalse(it.hasNext());

        // Latest checkpoints
        assertEquals(
                completed1.getCheckpointId(),
                snapshot.getHistory().getLatestCompletedCheckpoint().getCheckpointId());
        assertEquals(
                savepoint.getCheckpointId(),
                snapshot.getHistory().getLatestSavepoint().getCheckpointId());
        assertEquals(
                failed.getCheckpointId(),
                snapshot.getHistory().getLatestFailedCheckpoint().getCheckpointId());
        assertEquals(restored, snapshot.getLatestRestoredCheckpoint());
    }

    /** Tests that snapshots are only created if a new snapshot has been reported or updated. */
    @Test
    public void testCreateSnapshot() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(
                        10,
                        new UnregisteredMetricsGroup(),
                        new JobID(),
                        TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT_WITH_TASKS,
                        true);

        CheckpointStatsSnapshot snapshot1 = tracker.createSnapshot();

        // Pending checkpoint => new snapshot
        PendingCheckpointStats pending =
                tracker.reportPendingCheckpoint(
                        0,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        pending.reportSubtaskStats(jobVertexID, createSubtaskStats(0));

        CheckpointStatsSnapshot snapshot2 = tracker.createSnapshot();
        assertNotEquals(snapshot1, snapshot2);

        assertEquals(snapshot2, tracker.createSnapshot());

        // Complete checkpoint => new snapshot
        tracker.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(null));

        CheckpointStatsSnapshot snapshot3 = tracker.createSnapshot();
        assertNotEquals(snapshot2, snapshot3);

        // Restore operation => new snapshot
        tracker.reportInitializationStartTs(0);
        tracker.reportRestoredCheckpoint(
                new RestoredCheckpointStats(
                        12,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        12,
                        null,
                        42));

        CheckpointStatsSnapshot snapshot4 = tracker.createSnapshot();
        assertNotEquals(snapshot3, snapshot4);
        assertEquals(snapshot4, tracker.createSnapshot());
    }

    @Test
    public void testSpanCreationBreakDownPerCheckpoint() {
        testSpanCreationTemplate(TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT);
    }

    @Test
    public void testSpanCreationBreakDownPerCheckpointWithTasks() {
        testSpanCreationTemplate(
                TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT_WITH_TASKS);
    }

    @Test
    public void testSpanCreationBreakDownPerTask() {
        testSpanCreationTemplate(TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_TASK);
    }

    @Test
    public void testSpanCreationBreakDownPerSubTask() {
        testSpanCreationTemplate(TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_SUBTASK);
    }

    public void testSpanCreationTemplate(TraceOptions.CheckpointSpanDetailLevel detailLevel) {
        JobVertexID jobVertexID0 = new JobVertexID();
        JobVertexID jobVertexID1 = new JobVertexID();

        final List<Span> reportedSpans = new ArrayList<>();
        final List<Event> reportedEvents = new ArrayList<>();

        produceTestSpans(jobVertexID0, jobVertexID1, detailLevel, reportedSpans, reportedEvents);
        assertThat(reportedSpans.size()).isEqualTo(1);
        assertThat(reportedEvents.size()).isEqualTo(1);

        Map<String, Object> expected = new HashMap<>();
        expected.put("checkpointId", 42L);
        expected.put("checkpointedSize", 37L);
        expected.put("fullSize", 40L);
        expected.put("checkpointStatus", "COMPLETED");

        assertThat(reportedEvents.get(0).getAttributes())
                .containsExactlyInAnyOrderEntriesOf(expected);

        expected.put("maxCheckpointStartDelayMs", 29L);
        expected.put("maxPersistedDataBytes", 27L);
        expected.put("maxAsyncCheckpointDurationMs", 25L);
        expected.put("maxSyncCheckpointDurationMs", 24L);
        expected.put("maxAlignmentDurationMs", 28L);
        expected.put("maxProcessedDataBytes", 26L);
        expected.put("sumCheckpointStartDelayMs", 58L);
        expected.put("sumAlignmentDurationMs", 55L);
        expected.put("sumProcessedDataBytes", 49L);
        expected.put("sumAsyncCheckpointDurationMs", 46L);
        expected.put("sumPersistedDataBytes", 52L);
        expected.put("sumSyncCheckpointDurationMs", 43L);
        expected.put("maxStateSizeBytes", 23L);
        expected.put("maxCheckpointedSizeBytes", 22L);

        if (detailLevel == TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT_WITH_TASKS) {
            expected.put("perTaskMaxAlignmentDurationMs", "[28, 8]");
            expected.put("perTaskMaxAsyncCheckpointDurationMs", "[16, 25]");
            expected.put("perTaskMaxCheckpointStartDelayMs", "[20, 29]");
            expected.put("perTaskMaxPersistedDataBytes", "[18, 27]");
            expected.put("perTaskMaxProcessedDataBytes", "[17, 26]");
            expected.put("perTaskMaxSyncCheckpointDurationMs", "[24, 4]");
            expected.put("perTaskMaxStateSizeBytes", "[14, 23]");
            expected.put("perTaskMaxCheckpointedSizeBytes", "[13, 22]");
            expected.put("perTaskSumAlignmentDurationMs", "[47, 8]");
            expected.put("perTaskSumAsyncCheckpointDurationMs", "[21, 25]");
            expected.put("perTaskSumCheckpointStartDelayMs", "[29, 29]");
            expected.put("perTaskSumPersistedDataBytes", "[25, 27]");
            expected.put("perTaskSumProcessedDataBytes", "[23, 26]");
            expected.put("perTaskSumSyncCheckpointDurationMs", "[39, 4]");
            expected.put("perTaskSumStateSizeBytes", "[17, 23]");
            expected.put("perTaskSumCheckpointedSizeBytes", "[15, 22]");
        }

        Span checkpointLevelSpan = reportedSpans.get(0);
        assertThat(checkpointLevelSpan.getAttributes())
                .containsExactlyInAnyOrderEntriesOf(expected);

        List<Span> taskLevelSpans = checkpointLevelSpan.getChildren();
        if (detailLevel == TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_TASK
                || detailLevel == TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_SUBTASK) {
            assertThat(taskLevelSpans.size()).isEqualTo(2);
        } else {
            assertThat(taskLevelSpans).isEmpty();
            return;
        }

        Span taskSpan0 = taskLevelSpans.get(0);
        expected.clear();

        expected.put("checkpointId", 42L);
        expected.put("jobVertexId", jobVertexID0.toString());
        expected.put("maxCheckpointStartDelayMs", 20L);
        expected.put("maxPersistedDataBytes", 18L);
        expected.put("maxAsyncCheckpointDurationMs", 16L);
        expected.put("maxSyncCheckpointDurationMs", 24L);
        expected.put("maxAlignmentDurationMs", 28L);
        expected.put("maxProcessedDataBytes", 17L);
        expected.put("sumCheckpointStartDelayMs", 29L);
        expected.put("sumAlignmentDurationMs", 47L);
        expected.put("sumProcessedDataBytes", 23L);
        expected.put("sumAsyncCheckpointDurationMs", 21L);
        expected.put("sumPersistedDataBytes", 25L);
        expected.put("sumSyncCheckpointDurationMs", 39L);
        expected.put("maxStateSizeBytes", 14L);
        expected.put("sumStateSizeBytes", 17L);
        expected.put("maxCheckpointedSizeBytes", 13L);
        expected.put("sumCheckpointedSizeBytes", 15L);

        assertThat(taskSpan0.getAttributes()).containsExactlyInAnyOrderEntriesOf(expected);

        Span taskSpan1 = taskLevelSpans.get(1);
        expected.clear();

        expected.put("checkpointId", 42L);
        expected.put("jobVertexId", jobVertexID1.toString());
        expected.put("maxCheckpointStartDelayMs", 29L);
        expected.put("maxPersistedDataBytes", 27L);
        expected.put("maxAsyncCheckpointDurationMs", 25L);
        expected.put("maxSyncCheckpointDurationMs", 4L);
        expected.put("maxAlignmentDurationMs", 8L);
        expected.put("maxProcessedDataBytes", 26L);
        expected.put("sumCheckpointStartDelayMs", 29L);
        expected.put("sumAlignmentDurationMs", 8L);
        expected.put("sumProcessedDataBytes", 26L);
        expected.put("sumAsyncCheckpointDurationMs", 25L);
        expected.put("sumPersistedDataBytes", 27L);
        expected.put("sumSyncCheckpointDurationMs", 4L);
        expected.put("maxStateSizeBytes", 23L);
        expected.put("sumStateSizeBytes", 23L);
        expected.put("maxCheckpointedSizeBytes", 22L);
        expected.put("sumCheckpointedSizeBytes", 22L);
        assertThat(taskSpan1.getAttributes()).containsExactlyInAnyOrderEntriesOf(expected);

        List<Span> subtasksSpans0 = taskSpan0.getChildren();
        List<Span> subtasksSpans1 = taskSpan1.getChildren();

        if (detailLevel == TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_SUBTASK) {
            assertThat(subtasksSpans0.size()).isEqualTo(2);
            assertThat(subtasksSpans1.size()).isEqualTo(1);
        } else {
            assertThat(subtasksSpans0).isEmpty();
            assertThat(subtasksSpans1).isEmpty();
            return;
        }

        Span subtaskSpan0N0 = subtasksSpans0.get(0);
        expected.clear();

        expected.put("checkpointId", 42L);
        expected.put("jobVertexId", jobVertexID0.toString());
        expected.put("subtaskId", 0L);
        expected.put("CheckpointStartDelayMs", 9L);
        expected.put("AlignmentDurationMs", 28L);
        expected.put("ProcessedDataBytes", 6L);
        expected.put("AsyncCheckpointDurationMs", 5L);
        expected.put("PersistedDataBytes", 7L);
        expected.put("SyncCheckpointDurationMs", 24L);
        expected.put("StateSizeBytes", 3L);
        expected.put("CheckpointedSizeBytes", 2L);
        assertThat(subtaskSpan0N0.getAttributes()).containsExactlyInAnyOrderEntriesOf(expected);

        Span subtaskSpan0N1 = subtasksSpans0.get(1);
        expected.clear();

        expected.put("checkpointId", 42L);
        expected.put("jobVertexId", jobVertexID0.toString());
        expected.put("subtaskId", 1L);
        expected.put("CheckpointStartDelayMs", 20L);
        expected.put("AlignmentDurationMs", 19L);
        expected.put("ProcessedDataBytes", 17L);
        expected.put("AsyncCheckpointDurationMs", 16L);
        expected.put("PersistedDataBytes", 18L);
        expected.put("SyncCheckpointDurationMs", 15L);
        expected.put("StateSizeBytes", 14L);
        expected.put("CheckpointedSizeBytes", 13L);
        assertThat(subtaskSpan0N1.getAttributes()).containsExactlyInAnyOrderEntriesOf(expected);

        Span subtaskSpan1N10 = subtasksSpans1.get(0);
        expected.clear();

        expected.put("checkpointId", 42L);
        expected.put("jobVertexId", jobVertexID1.toString());
        expected.put("subtaskId", 0L);
        expected.put("CheckpointStartDelayMs", 29L);
        expected.put("AlignmentDurationMs", 8L);
        expected.put("ProcessedDataBytes", 26L);
        expected.put("AsyncCheckpointDurationMs", 25L);
        expected.put("PersistedDataBytes", 27L);
        expected.put("SyncCheckpointDurationMs", 4L);
        expected.put("StateSizeBytes", 23L);
        expected.put("CheckpointedSizeBytes", 22L);
        assertThat(subtaskSpan1N10.getAttributes()).containsExactlyInAnyOrderEntriesOf(expected);
    }

    private List<Span> produceTestSpans(
            JobVertexID jobVertexID0,
            JobVertexID jobVertexID1,
            TraceOptions.CheckpointSpanDetailLevel detailLevel,
            List<Span> reportedSpansOut,
            List<Event> reportedEventsOut) {

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public void addSpan(SpanBuilder spanBuilder) {
                        reportedSpansOut.add(spanBuilder.build());
                    }

                    @Override
                    public void addEvent(EventBuilder eventBuilder) {
                        reportedEventsOut.add(eventBuilder.build());
                    }
                };

        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(10, metricGroup, JobID.generate(), detailLevel, true);

        Map<JobVertexID, Integer> subtasksByVertex = CollectionUtil.newHashMapWithExpectedSize(2);
        subtasksByVertex.put(jobVertexID0, 2);
        subtasksByVertex.put(jobVertexID1, 1);
        PendingCheckpointStats pending =
                tracker.reportPendingCheckpoint(
                        42,
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        subtasksByVertex);

        pending.reportSubtaskStats(
                jobVertexID0, new SubtaskStateStats(0, 1, 2, 3, 24, 5, 6, 7, 28, 9, false, true));
        pending.reportSubtaskStats(
                jobVertexID0,
                new SubtaskStateStats(1, 12, 13, 14, 15, 16, 17, 18, 19, 20, false, true));
        pending.reportSubtaskStats(
                jobVertexID1,
                new SubtaskStateStats(0, 21, 22, 23, 4, 25, 26, 27, 8, 29, false, true));
        // Complete checkpoint => new snapshot
        tracker.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(null));
        return reportedSpansOut;
    }

    @Test
    public void testInitializationSpanCreation() {
        final List<Span> reportedSpans = new ArrayList<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public void addSpan(SpanBuilder spanBuilder) {
                        reportedSpans.add(spanBuilder.build());
                    }
                };

        CheckpointStatsTracker tracker =
                new CheckpointStatsTracker(
                        10,
                        metricGroup,
                        new JobID(),
                        2,
                        TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT_WITH_TASKS,
                        true);

        tracker.reportInitializationStartTs(100);
        tracker.reportRestoredCheckpoint(
                new RestoredCheckpointStats(
                        42,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        100,
                        null,
                        1024));

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder3 =
                new SubTaskInitializationMetricsBuilder(110)
                        .setStatus(InitializationStatus.COMPLETED);
        subTaskInitializationMetricsBuilder3.addDurationMetric("MailboxStartDurationMs", 10);
        subTaskInitializationMetricsBuilder3.addDurationMetric("ReadOutputDataDurationMs", 20);
        subTaskInitializationMetricsBuilder3.addDurationMetric("InitializeStateDurationMs", 30);
        subTaskInitializationMetricsBuilder3.addDurationMetric("GateRestoreDurationMs", 40);
        tracker.reportInitializationMetrics(subTaskInitializationMetricsBuilder3.build(215));
        assertThat(reportedSpans).isEmpty();

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder2 =
                new SubTaskInitializationMetricsBuilder(110)
                        .setStatus(InitializationStatus.COMPLETED);
        subTaskInitializationMetricsBuilder2.addDurationMetric("MailboxStartDurationMs", 10);
        subTaskInitializationMetricsBuilder2.addDurationMetric("ReadOutputDataDurationMs", 20);
        subTaskInitializationMetricsBuilder2.addDurationMetric("InitializeStateDurationMs", 30);
        subTaskInitializationMetricsBuilder2.addDurationMetric("GateRestoreDurationMs", 40);
        tracker.reportInitializationMetrics(subTaskInitializationMetricsBuilder2.build(215));

        assertThat(reportedSpans.size()).isEqualTo(1);
        Span reportedSpan = Iterables.getOnlyElement(reportedSpans);
        assertThat(reportedSpan.getStartTsMillis()).isEqualTo(100L);
        assertThat(reportedSpan.getEndTsMillis()).isEqualTo(215L);
        assertThat(reportedSpan.getAttributes().get("checkpointId")).isEqualTo(42L);
        assertThat(reportedSpan.getAttributes().get("fullSize")).isEqualTo(1024L);

        // simulate another failover with the same instance
        reportedSpans.clear();

        tracker.reportInitializationStartTs(100);
        tracker.reportRestoredCheckpoint(
                new RestoredCheckpointStats(
                        44,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
                        100,
                        null,
                        1024));

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder1 =
                new SubTaskInitializationMetricsBuilder(110)
                        .setStatus(InitializationStatus.COMPLETED);
        subTaskInitializationMetricsBuilder1.addDurationMetric("MailboxStartDurationMs", 10);
        subTaskInitializationMetricsBuilder1.addDurationMetric("ReadOutputDataDurationMs", 20);
        subTaskInitializationMetricsBuilder1.addDurationMetric("InitializeStateDurationMs", 30);
        subTaskInitializationMetricsBuilder1.addDurationMetric("GateRestoreDurationMs", 40);
        tracker.reportInitializationMetrics(subTaskInitializationMetricsBuilder1.build(215));
        assertThat(reportedSpans).isEmpty();

        SubTaskInitializationMetricsBuilder subTaskInitializationMetricsBuilder =
                new SubTaskInitializationMetricsBuilder(110)
                        .setStatus(InitializationStatus.COMPLETED);
        subTaskInitializationMetricsBuilder.addDurationMetric("MailboxStartDurationMs", 10);
        subTaskInitializationMetricsBuilder.addDurationMetric("ReadOutputDataDurationMs", 20);
        subTaskInitializationMetricsBuilder.addDurationMetric("InitializeStateDurationMs", 30);
        subTaskInitializationMetricsBuilder.addDurationMetric("GateRestoreDurationMs", 40);
        tracker.reportInitializationMetrics(subTaskInitializationMetricsBuilder.build(215));

        assertThat(reportedSpans.size()).isEqualTo(1);
        reportedSpan = Iterables.getOnlyElement(reportedSpans);
        assertThat(reportedSpan.getAttributes().get("checkpointId")).isEqualTo(44L);
    }

    /** Tests the registration of the checkpoint metrics. */
    @Test
    public void testMetricsRegistration() throws Exception {
        final Collection<String> registeredGaugeNames = new ArrayList<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        if (gauge != null) {
                            registeredGaugeNames.add(name);
                        }
                        return gauge;
                    }
                };

        new CheckpointStatsTracker(
                0,
                metricGroup,
                new JobID(),
                TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT_WITH_TASKS,
                true);

        // Make sure this test is adjusted when further metrics are added
        assertTrue(
                registeredGaugeNames.containsAll(
                        Arrays.asList(
                                CheckpointStatsTracker.NUMBER_OF_CHECKPOINTS_METRIC,
                                CheckpointStatsTracker.NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC,
                                CheckpointStatsTracker.NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC,
                                CheckpointStatsTracker.NUMBER_OF_FAILED_CHECKPOINTS_METRIC,
                                CheckpointStatsTracker.LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC,
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC,
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_FULL_SIZE_METRIC,
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC,
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_PROCESSED_DATA_METRIC,
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_PERSISTED_DATA_METRIC,
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC,
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_ID_METRIC)));
        assertEquals(12, registeredGaugeNames.size());
    }

    /**
     * Tests that the metrics are updated properly. We had a bug that required new stats snapshots
     * in order to update the metrics.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testMetricsAreUpdated() throws Exception {
        final Map<String, Gauge<?>> registeredGauges = new HashMap<>();

        MetricGroup metricGroup =
                new UnregisteredMetricsGroup() {
                    @Override
                    public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
                        registeredGauges.put(name, gauge);
                        return gauge;
                    }
                };

        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex = graph.getJobVertex(jobVertexID);

        CheckpointStatsTracker stats =
                new CheckpointStatsTracker(
                        0,
                        metricGroup,
                        new JobID(),
                        TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_CHECKPOINT_WITH_TASKS,
                        true);

        // Make sure to adjust this test if metrics are added/removed
        assertEquals(12, registeredGauges.size());

        // Check initial values
        Gauge<Long> numCheckpoints =
                (Gauge<Long>)
                        registeredGauges.get(CheckpointStatsTracker.NUMBER_OF_CHECKPOINTS_METRIC);
        Gauge<Integer> numInProgressCheckpoints =
                (Gauge<Integer>)
                        registeredGauges.get(
                                CheckpointStatsTracker.NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC);
        Gauge<Long> numCompletedCheckpoints =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC);
        Gauge<Long> numFailedCheckpoints =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.NUMBER_OF_FAILED_CHECKPOINTS_METRIC);
        Gauge<Long> latestRestoreTimestamp =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC);
        Gauge<Long> latestCompletedSize =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC);
        Gauge<Long> latestCompletedFullSize =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_FULL_SIZE_METRIC);
        Gauge<Long> latestCompletedDuration =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC);
        Gauge<Long> latestProcessedData =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_PROCESSED_DATA_METRIC);
        Gauge<Long> latestPersistedData =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_PERSISTED_DATA_METRIC);
        Gauge<String> latestCompletedExternalPath =
                (Gauge<String>)
                        registeredGauges.get(
                                CheckpointStatsTracker
                                        .LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC);
        Gauge<Long> latestCompletedId =
                (Gauge<Long>)
                        registeredGauges.get(
                                CheckpointStatsTracker.LATEST_COMPLETED_CHECKPOINT_ID_METRIC);

        assertEquals(Long.valueOf(0), numCheckpoints.getValue());
        assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
        assertEquals(Long.valueOf(0), numCompletedCheckpoints.getValue());
        assertEquals(Long.valueOf(0), numFailedCheckpoints.getValue());
        assertEquals(Long.valueOf(-1), latestRestoreTimestamp.getValue());
        assertEquals(Long.valueOf(-1), latestCompletedSize.getValue());
        assertEquals(Long.valueOf(-1), latestCompletedFullSize.getValue());
        assertEquals(Long.valueOf(-1), latestCompletedDuration.getValue());
        assertEquals(Long.valueOf(-1), latestProcessedData.getValue());
        assertEquals(Long.valueOf(-1), latestPersistedData.getValue());
        assertEquals("n/a", latestCompletedExternalPath.getValue());
        assertEquals(Long.valueOf(-1), latestCompletedId.getValue());

        PendingCheckpointStats pending =
                stats.reportPendingCheckpoint(
                        0,
                        0,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        // Check counts
        assertEquals(Long.valueOf(1), numCheckpoints.getValue());
        assertEquals(Integer.valueOf(1), numInProgressCheckpoints.getValue());
        assertEquals(Long.valueOf(0), numCompletedCheckpoints.getValue());
        assertEquals(Long.valueOf(0), numFailedCheckpoints.getValue());

        long ackTimestamp = 11231230L;
        long checkpointedSize = 123L;
        long fullCheckpointSize = 12381238L;
        long processedData = 4242L;
        long persistedData = 4444L;
        long ignored = 0;
        String externalPath = "myexternalpath";

        SubtaskStateStats subtaskStats =
                new SubtaskStateStats(
                        0,
                        ackTimestamp,
                        checkpointedSize,
                        fullCheckpointSize,
                        ignored,
                        ignored,
                        processedData,
                        persistedData,
                        ignored,
                        ignored,
                        false,
                        true);

        assertTrue(pending.reportSubtaskStats(jobVertexID, subtaskStats));

        stats.reportCompletedCheckpoint(pending.toCompletedCheckpointStats(externalPath));

        // Verify completed checkpoint updated
        assertEquals(Long.valueOf(1), numCheckpoints.getValue());
        assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
        assertEquals(Long.valueOf(1), numCompletedCheckpoints.getValue());
        assertEquals(Long.valueOf(0), numFailedCheckpoints.getValue());
        assertEquals(Long.valueOf(-1), latestRestoreTimestamp.getValue());
        assertEquals(Long.valueOf(checkpointedSize), latestCompletedSize.getValue());
        assertEquals(Long.valueOf(fullCheckpointSize), latestCompletedFullSize.getValue());
        assertEquals(Long.valueOf(processedData), latestProcessedData.getValue());
        assertEquals(Long.valueOf(persistedData), latestPersistedData.getValue());
        assertEquals(Long.valueOf(ackTimestamp), latestCompletedDuration.getValue());
        assertEquals(externalPath, latestCompletedExternalPath.getValue());
        assertEquals(Long.valueOf(0), latestCompletedId.getValue());

        // Check failed
        PendingCheckpointStats nextPending =
                stats.reportPendingCheckpoint(
                        1,
                        11,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        long failureTimestamp = 1230123L;
        stats.reportFailedCheckpoint(nextPending.toFailedCheckpoint(failureTimestamp, null));

        // Verify updated
        assertEquals(Long.valueOf(2), numCheckpoints.getValue());
        assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
        assertEquals(Long.valueOf(1), numCompletedCheckpoints.getValue());
        assertEquals(Long.valueOf(1), numFailedCheckpoints.getValue()); // one failed now
        assertEquals(Long.valueOf(0), latestCompletedId.getValue());

        // Check restore
        long restoreTimestamp = 183419283L;
        RestoredCheckpointStats restored =
                new RestoredCheckpointStats(
                        1,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        restoreTimestamp,
                        null,
                        42);
        stats.reportInitializationStartTs(restoreTimestamp);
        stats.reportRestoredCheckpoint(restored);

        assertEquals(Long.valueOf(2), numCheckpoints.getValue());
        assertEquals(Integer.valueOf(0), numInProgressCheckpoints.getValue());
        assertEquals(Long.valueOf(1), numCompletedCheckpoints.getValue());
        assertEquals(Long.valueOf(1), numFailedCheckpoints.getValue());
        assertEquals(Long.valueOf(0), latestCompletedId.getValue());

        assertEquals(Long.valueOf(restoreTimestamp), latestRestoreTimestamp.getValue());

        // Check Internal Checkpoint Configuration
        PendingCheckpointStats thirdPending =
                stats.reportPendingCheckpoint(
                        2,
                        5000,
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        singletonMap(jobVertexID, 1));

        thirdPending.reportSubtaskStats(jobVertexID, subtaskStats);
        stats.reportCompletedCheckpoint(thirdPending.toCompletedCheckpointStats(null));
        assertEquals(Long.valueOf(2), latestCompletedId.getValue());

        // Verify external path is "n/a", because internal checkpoint won't generate external path.
        assertEquals("n/a", latestCompletedExternalPath.getValue());
    }

    // ------------------------------------------------------------------------

    private SubtaskStateStats createSubtaskStats(int index) {
        return new SubtaskStateStats(index, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, true);
    }
}
