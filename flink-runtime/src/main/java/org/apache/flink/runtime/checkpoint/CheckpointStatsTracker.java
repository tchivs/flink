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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatistics;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.runtime.util.LongArrayList;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Tracker for checkpoint statistics.
 *
 * <p>This is tightly integrated with the {@link CheckpointCoordinator} in order to ease the
 * gathering of fine-grained statistics.
 *
 * <p>The tracked stats include summary counts, a detailed history of recent and in progress
 * checkpoints as well as summaries about the size, duration and more of recent checkpoints.
 *
 * <p>Data is gathered via callbacks in the {@link CheckpointCoordinator} and related classes like
 * {@link PendingCheckpoint} and {@link CompletedCheckpoint}, which receive the raw stats data in
 * the first place.
 *
 * <p>The statistics are accessed via {@link #createSnapshot()} and exposed via both the web
 * frontend and the {@link Metric} system.
 */
public class CheckpointStatsTracker {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointStatsTracker.class);
    private static final ObjectMapper MAPPER = RestMapperUtils.getStrictObjectMapper();

    /**
     * Function that extracts a {@link StatsSummary} from a {@link
     * org.apache.flink.runtime.checkpoint.TaskStateStats.TaskStateStatsSummary}.
     */
    @FunctionalInterface
    interface TaskStatsSummaryExtractor {
        StatsSummary extract(TaskStateStats.TaskStateStatsSummary taskStateStatsSummary);
    }

    /** Function that extracts a (long) metric value from {@link SubtaskStateStats}. */
    @FunctionalInterface
    interface SubtaskMetricExtractor {
        long extract(SubtaskStateStats subtaskStateStats);
    }

    /**
     * Helper class that defines a checkpoint span metric and how to extract the required values.
     */
    static final class CheckpointSpanMetric {
        final String metricName;
        final TaskStatsSummaryExtractor taskStatsSummaryExtractor;
        final SubtaskMetricExtractor subtaskMetricExtractor;

        private CheckpointSpanMetric(
                String metricName,
                TaskStatsSummaryExtractor taskStatsSummaryExtractor,
                SubtaskMetricExtractor subtaskMetricExtractor) {
            this.metricName = metricName;
            this.taskStatsSummaryExtractor = taskStatsSummaryExtractor;
            this.subtaskMetricExtractor = subtaskMetricExtractor;
        }

        static CheckpointSpanMetric of(
                String metricName,
                TaskStatsSummaryExtractor taskStatsSummaryExtractor,
                SubtaskMetricExtractor subtaskMetricExtractor) {
            return new CheckpointSpanMetric(
                    metricName, taskStatsSummaryExtractor, subtaskMetricExtractor);
        }
    }

    private static final List<CheckpointSpanMetric> CHECKPOINT_SPAN_METRICS =
            Arrays.asList(
                    CheckpointSpanMetric.of(
                            "StateSizeBytes",
                            TaskStateStats.TaskStateStatsSummary::getStateSizeStats,
                            SubtaskStateStats::getStateSize),
                    CheckpointSpanMetric.of(
                            "CheckpointedSizeBytes",
                            TaskStateStats.TaskStateStatsSummary::getCheckpointedSize,
                            SubtaskStateStats::getCheckpointedSize),
                    CheckpointSpanMetric.of(
                            "CheckpointStartDelayMs",
                            TaskStateStats.TaskStateStatsSummary::getCheckpointStartDelayStats,
                            SubtaskStateStats::getCheckpointStartDelay),
                    CheckpointSpanMetric.of(
                            "AlignmentDurationMs",
                            TaskStateStats.TaskStateStatsSummary::getAlignmentDurationStats,
                            SubtaskStateStats::getAlignmentDuration),
                    CheckpointSpanMetric.of(
                            "SyncCheckpointDurationMs",
                            TaskStateStats.TaskStateStatsSummary::getSyncCheckpointDurationStats,
                            SubtaskStateStats::getSyncCheckpointDuration),
                    CheckpointSpanMetric.of(
                            "AsyncCheckpointDurationMs",
                            TaskStateStats.TaskStateStatsSummary::getAsyncCheckpointDurationStats,
                            SubtaskStateStats::getAsyncCheckpointDuration),
                    CheckpointSpanMetric.of(
                            "ProcessedDataBytes",
                            TaskStateStats.TaskStateStatsSummary::getProcessedDataStats,
                            SubtaskStateStats::getProcessedData),
                    CheckpointSpanMetric.of(
                            "PersistedDataBytes",
                            TaskStateStats.TaskStateStatsSummary::getPersistedDataStats,
                            SubtaskStateStats::getPersistedData));
    private final TraceOptions.CheckpointSpanDetailLevel checkpointSpanDetailLevel;

    /**
     * Creates a new checkpoint stats tracker.
     *
     * @param numRememberedCheckpoints Maximum number of checkpoints to remember, including in
     *     progress ones.
     * @param metricGroup Metric group for exposed metrics
     * @param jobID ID of the job being checkpointed
     */
    public CheckpointStatsTracker(
            int numRememberedCheckpoints,
            MetricGroup metricGroup,
            JobID jobID,
            TraceOptions.CheckpointSpanDetailLevel checkpointSpanDetailLevel) {
        this(
                numRememberedCheckpoints,
                metricGroup,
                jobID,
                Integer.MAX_VALUE,
                checkpointSpanDetailLevel);
    }

    /**
     * Lock used to update stats and creating snapshots. Updates always happen from a single Thread
     * at a time and there can be multiple concurrent read accesses to the latest stats snapshot.
     *
     * <p>Currently, writes are executed by whatever Thread executes the coordinator actions (which
     * already happens in locked scope). Reads can come from multiple concurrent Netty event loop
     * Threads of the web runtime monitor.
     */
    private final ReentrantLock statsReadWriteLock = new ReentrantLock();

    /** Checkpoint counts. */
    private final CheckpointStatsCounts counts = new CheckpointStatsCounts();

    /** A summary of the completed checkpoint stats. */
    private final CompletedCheckpointStatsSummary summary = new CompletedCheckpointStatsSummary();

    /** History of checkpoints. */
    private final CheckpointStatsHistory history;

    private final JobID jobID;
    private final MetricGroup metricGroup;
    private int totalNumberOfSubTasks;

    private Optional<JobInitializationMetricsBuilder> jobInitializationMetricsBuilder =
            Optional.empty();

    /** Latest created snapshot. */
    private volatile CheckpointStatsSnapshot latestSnapshot;

    /**
     * Flag indicating whether a new snapshot needs to be created. This is true if a new checkpoint
     * was triggered or updated (completed successfully or failed).
     */
    private volatile boolean dirty;

    /** The latest completed checkpoint. Used by the latest completed checkpoint metrics. */
    @Nullable private volatile CompletedCheckpointStats latestCompletedCheckpoint;

    CheckpointStatsTracker(
            int numRememberedCheckpoints,
            MetricGroup metricGroup,
            JobID jobID,
            int totalNumberOfSubTasks,
            TraceOptions.CheckpointSpanDetailLevel checkpointSpanDetailLevel) {
        checkArgument(numRememberedCheckpoints >= 0, "Negative number of remembered checkpoints");
        this.history = new CheckpointStatsHistory(numRememberedCheckpoints);
        this.jobID = jobID;
        this.metricGroup = metricGroup;
        this.totalNumberOfSubTasks = totalNumberOfSubTasks;
        this.checkpointSpanDetailLevel = checkpointSpanDetailLevel;

        // Latest snapshot is empty
        latestSnapshot =
                new CheckpointStatsSnapshot(
                        counts.createSnapshot(),
                        summary.createSnapshot(),
                        history.createSnapshot(),
                        null);

        // Register the metrics
        registerMetrics(metricGroup);
    }

    private void addCheckpointAggregationStats(
            AbstractCheckpointStats checkpointStats, SpanBuilder checkpointSpanBuilder) {

        final List<TaskStateStats> sortedTaskStateStats =
                new ArrayList<>(checkpointStats.getAllTaskStateStats());
        sortedTaskStateStats.sort(
                (x, y) ->
                        Long.signum(
                                x.getSummaryStats().getCheckpointStartDelayStats().getMinimum()
                                        - y.getSummaryStats()
                                                .getCheckpointStartDelayStats()
                                                .getMinimum()));

        CHECKPOINT_SPAN_METRICS.stream()
                .map(metric -> TaskStatsAggregator.aggregate(sortedTaskStateStats, metric))
                .forEach(
                        aggregator -> {
                            final String metricName = aggregator.getMetricName();
                            checkpointSpanBuilder.setAttribute(
                                    "max" + metricName, aggregator.getTotalMax());

                            if (!shouldSkipSumMetricNameInCheckpointSpanForCompatibility(
                                    metricName)) {
                                checkpointSpanBuilder.setAttribute(
                                        "sum" + metricName, aggregator.getTotalSum());
                            }

                            if (checkpointSpanDetailLevel
                                    == TraceOptions.CheckpointSpanDetailLevel
                                            .SPANS_PER_CHECKPOINT_WITH_TASKS) {
                                checkpointSpanBuilder.setAttribute(
                                        "perTaskMax" + metricName,
                                        Arrays.toString(
                                                aggregator.getValuesMax().getInternalArray()));
                                checkpointSpanBuilder.setAttribute(
                                        "perTaskSum" + metricName,
                                        Arrays.toString(
                                                aggregator.getValuesSum().getInternalArray()));
                            }
                        });

        if (checkpointSpanDetailLevel == TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_TASK
                || checkpointSpanDetailLevel
                        == TraceOptions.CheckpointSpanDetailLevel.SPANS_PER_SUBTASK) {
            for (TaskStateStats taskStats : sortedTaskStateStats) {
                checkpointSpanBuilder.addChild(
                        createTaskSpan(
                                checkpointStats,
                                taskStats,
                                checkpointSpanDetailLevel
                                        == TraceOptions.CheckpointSpanDetailLevel
                                                .SPANS_PER_SUBTASK));
            }
        }
    }

    public CheckpointStatsTracker updateTotalNumberOfSubtasks(int totalNumberOfSubTasks) {
        this.totalNumberOfSubTasks = totalNumberOfSubTasks;
        return this;
    }

    @VisibleForTesting
    Optional<JobInitializationMetricsBuilder> getJobInitializationMetricsBuilder() {
        return jobInitializationMetricsBuilder;
    }

    /**
     * Creates a new snapshot of the available stats.
     *
     * @return The latest statistics snapshot.
     */
    public CheckpointStatsSnapshot createSnapshot() {
        CheckpointStatsSnapshot snapshot = latestSnapshot;

        // Only create a new snapshot if dirty and no update in progress,
        // because we don't want to block the coordinator.
        if (dirty && statsReadWriteLock.tryLock()) {
            try {
                // Create a new snapshot
                snapshot =
                        new CheckpointStatsSnapshot(
                                counts.createSnapshot(),
                                summary.createSnapshot(),
                                history.createSnapshot(),
                                jobInitializationMetricsBuilder
                                        .flatMap(
                                                JobInitializationMetricsBuilder
                                                        ::buildRestoredCheckpointStats)
                                        .orElse(null));

                latestSnapshot = snapshot;

                dirty = false;
            } finally {
                statsReadWriteLock.unlock();
            }
        }

        return snapshot;
    }

    private Span createTaskSpan(
            AbstractCheckpointStats checkpointStats,
            TaskStateStats taskStats,
            boolean addSubtaskSpans) {

        // start = trigger ts + minimum delay.
        long taskStartTs =
                checkpointStats.getTriggerTimestamp()
                        + taskStats.getSummaryStats().getCheckpointStartDelayStats().getMinimum();
        SpanBuilder taskSpanBuilder =
                Span.builder(CheckpointStatsTracker.class, "Checkpoint_Task")
                        .setStartTsMillis(taskStartTs)
                        .setEndTsMillis(taskStats.getLatestAckTimestamp())
                        .setAttribute("checkpointId", checkpointStats.getCheckpointId())
                        .setAttribute("jobVertexId", taskStats.getJobVertexId().toString());

        for (CheckpointSpanMetric spanMetric : CHECKPOINT_SPAN_METRICS) {
            String metricName = spanMetric.metricName;
            StatsSummary statsSummary =
                    spanMetric.taskStatsSummaryExtractor.extract(taskStats.getSummaryStats());
            taskSpanBuilder.setAttribute("max" + metricName, statsSummary.getMaximum());
            taskSpanBuilder.setAttribute("sum" + metricName, statsSummary.getSum());
        }

        if (addSubtaskSpans) {
            addSubtaskSpans(checkpointStats, taskStats, taskSpanBuilder);
        }

        return taskSpanBuilder.build();
    }

    // ------------------------------------------------------------------------
    // Callbacks
    // ------------------------------------------------------------------------

    /**
     * Creates a new pending checkpoint tracker.
     *
     * @param checkpointId ID of the checkpoint.
     * @param triggerTimestamp Trigger timestamp of the checkpoint.
     * @param props The checkpoint properties.
     * @param vertexToDop mapping of {@link JobVertexID} to DOP
     * @return Tracker for statistics gathering.
     */
    PendingCheckpointStats reportPendingCheckpoint(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            Map<JobVertexID, Integer> vertexToDop) {

        PendingCheckpointStats pending =
                new PendingCheckpointStats(checkpointId, triggerTimestamp, props, vertexToDop);

        statsReadWriteLock.lock();
        try {
            counts.incrementInProgressCheckpoints();
            history.addInProgressCheckpoint(pending);

            dirty = true;
        } finally {
            statsReadWriteLock.unlock();
        }

        return pending;
    }

    /**
     * Callback when a checkpoint is restored.
     *
     * @param restored The restored checkpoint stats.
     */
    @Deprecated
    void reportRestoredCheckpoint(RestoredCheckpointStats restored) {
        checkNotNull(restored, "Restored checkpoint");
        reportRestoredCheckpoint(
                restored.getCheckpointId(),
                restored.getProperties(),
                restored.getExternalPath(),
                restored.getStateSize());
    }

    public void reportRestoredCheckpoint(
            long checkpointID,
            CheckpointProperties properties,
            String externalPath,
            long stateSize) {
        statsReadWriteLock.lock();
        try {
            counts.incrementRestoredCheckpoints();
            checkState(
                    jobInitializationMetricsBuilder.isPresent(),
                    "JobInitializationMetrics should have been set first, before RestoredCheckpointStats");
            jobInitializationMetricsBuilder
                    .get()
                    .setRestoredCheckpointStats(checkpointID, stateSize, properties, externalPath);
            dirty = true;
        } finally {
            statsReadWriteLock.unlock();
        }
    }

    /**
     * Callback when a checkpoint completes.
     *
     * @param completed The completed checkpoint stats.
     */
    void reportCompletedCheckpoint(CompletedCheckpointStats completed) {
        statsReadWriteLock.lock();
        try {
            latestCompletedCheckpoint = completed;

            counts.incrementCompletedCheckpoints();
            history.replacePendingCheckpointById(completed);

            summary.updateSummary(completed);

            dirty = true;
            logCheckpointStatistics(completed);
        } finally {
            statsReadWriteLock.unlock();
        }
    }

    /**
     * Callback when a checkpoint fails.
     *
     * @param failed The failed checkpoint stats.
     */
    void reportFailedCheckpoint(FailedCheckpointStats failed) {
        statsReadWriteLock.lock();
        try {
            counts.incrementFailedCheckpoints();
            history.replacePendingCheckpointById(failed);

            dirty = true;
            logCheckpointStatistics(failed);
        } finally {
            statsReadWriteLock.unlock();
        }
    }

    private void logCheckpointStatistics(AbstractCheckpointStats checkpointStats) {
        try {

            // Create span with top level metrics
            SpanBuilder spanBuilder =
                    Span.builder(CheckpointStatsTracker.class, "Checkpoint")
                            .setStartTsMillis(checkpointStats.getTriggerTimestamp())
                            .setEndTsMillis(checkpointStats.getLatestAckTimestamp())
                            .setAttribute("checkpointId", checkpointStats.getCheckpointId())
                            .setAttribute("fullSize", checkpointStats.getStateSize())
                            .setAttribute("checkpointedSize", checkpointStats.getCheckpointedSize())
                            .setAttribute("checkpointStatus", checkpointStats.getStatus().name());

            // Add max/sum aggregations for breakdown metrics
            addCheckpointAggregationStats(checkpointStats, spanBuilder);

            metricGroup.addSpan(spanBuilder);

            if (LOG.isDebugEnabled()) {
                StringWriter sw = new StringWriter();
                MAPPER.writeValue(
                        sw,
                        CheckpointStatistics.generateCheckpointStatistics(checkpointStats, true));
                String jsonDump = sw.toString();
                LOG.debug(
                        "CheckpointStatistics (for jobID={}, checkpointId={}) dump = {} ",
                        jobID,
                        checkpointStats.checkpointId,
                        jsonDump);
            }
        } catch (Exception ex) {
            LOG.warn("Fail to log CheckpointStatistics", ex);
        }
    }

    private void addSubtaskSpans(
            AbstractCheckpointStats checkpointStats,
            TaskStateStats taskStats,
            SpanBuilder taskSpanBuilder) {
        for (SubtaskStateStats subtaskStat : taskStats.getSubtaskStats()) {
            if (subtaskStat == null) {
                continue;
            }

            // start = trigger ts + minimum delay.
            long subTaskStartTs =
                    checkpointStats.getTriggerTimestamp() + subtaskStat.getCheckpointStartDelay();

            SpanBuilder subTaskSpanBuilder =
                    Span.builder(CheckpointStatsTracker.class, "Checkpoint_Subtask")
                            .setStartTsMillis(subTaskStartTs)
                            .setEndTsMillis(subtaskStat.getAckTimestamp())
                            .setAttribute("checkpointId", checkpointStats.getCheckpointId())
                            .setAttribute("jobVertexId", taskStats.getJobVertexId().toString())
                            .setAttribute("subtaskId", subtaskStat.getSubtaskIndex());

            for (CheckpointSpanMetric spanMetric : CHECKPOINT_SPAN_METRICS) {
                String metricName = spanMetric.metricName;
                long metricValue = spanMetric.subtaskMetricExtractor.extract(subtaskStat);
                subTaskSpanBuilder.setAttribute(metricName, metricValue);
            }

            taskSpanBuilder.addChild(subTaskSpanBuilder.build());
        }
    }

    private boolean shouldSkipSumMetricNameInCheckpointSpanForCompatibility(String metricName) {
        // Those two metrics already exists under different names that we want to preserve
        // (fullSize, checkpointedSize).
        return metricName.equals("StateSizeBytes") || metricName.equals("CheckpointedSizeBytes");
    }

    static class TaskStatsAggregator {

        final String metricName;
        final LongArrayList valuesMax;
        final LongArrayList valuesSum;

        TaskStatsAggregator(String metric, LongArrayList valuesMax, LongArrayList valuesSum) {
            this.metricName = metric;
            this.valuesMax = valuesMax;
            this.valuesSum = valuesSum;
        }

        public static TaskStatsAggregator aggregate(
                Collection<TaskStateStats> allTaskStateStats,
                CheckpointSpanMetric metricDescriptor) {

            final LongArrayList valuesMax = new LongArrayList(allTaskStateStats.size());
            final LongArrayList valuesSum = new LongArrayList(allTaskStateStats.size());
            for (TaskStateStats taskStats : allTaskStateStats) {
                StatsSummary statsSummary =
                        metricDescriptor.taskStatsSummaryExtractor.extract(
                                taskStats.getSummaryStats());
                valuesMax.add(statsSummary.getMaximum());
                valuesSum.add(statsSummary.getSum());
            }
            return new TaskStatsAggregator(metricDescriptor.metricName, valuesMax, valuesSum);
        }

        public LongArrayList getValuesMax() {
            return valuesMax;
        }

        public LongArrayList getValuesSum() {
            return valuesSum;
        }

        public String getMetricName() {
            return metricName;
        }

        public long getTotalMax() {
            return Arrays.stream(valuesMax.getInternalArray())
                    .filter(val -> val > 0L)
                    .max()
                    .orElse(0L);
        }

        public long getTotalSum() {
            return Arrays.stream(valuesSum.getInternalArray()).filter(val -> val >= 0L).sum();
        }
    }

    /**
     * Callback when a checkpoint failure without in progress checkpoint. For example, it should be
     * callback when triggering checkpoint failure before creating PendingCheckpoint.
     */
    public void reportFailedCheckpointsWithoutInProgress() {
        statsReadWriteLock.lock();
        try {
            counts.incrementFailedCheckpointsWithoutInProgress();

            dirty = true;
        } finally {
            statsReadWriteLock.unlock();
        }
    }

    public PendingCheckpointStats getPendingCheckpointStats(long checkpointId) {
        statsReadWriteLock.lock();
        try {
            AbstractCheckpointStats stats = history.getCheckpointById(checkpointId);
            return stats instanceof PendingCheckpointStats ? (PendingCheckpointStats) stats : null;
        } finally {
            statsReadWriteLock.unlock();
        }
    }

    public void reportIncompleteStats(
            long checkpointId, ExecutionAttemptID attemptId, CheckpointMetrics metrics) {
        statsReadWriteLock.lock();
        try {
            AbstractCheckpointStats stats = history.getCheckpointById(checkpointId);
            if (stats instanceof PendingCheckpointStats) {
                ((PendingCheckpointStats) stats)
                        .reportSubtaskStats(
                                attemptId.getJobVertexId(),
                                new SubtaskStateStats(
                                        attemptId.getSubtaskIndex(),
                                        System.currentTimeMillis(),
                                        metrics.getBytesPersistedOfThisCheckpoint(),
                                        metrics.getTotalBytesPersisted(),
                                        metrics.getSyncDurationMillis(),
                                        metrics.getAsyncDurationMillis(),
                                        metrics.getBytesProcessedDuringAlignment(),
                                        metrics.getBytesPersistedDuringAlignment(),
                                        metrics.getAlignmentDurationNanos() / 1_000_000,
                                        metrics.getCheckpointStartDelayNanos() / 1_000_000,
                                        metrics.getUnalignedCheckpoint(),
                                        false));
                dirty = true;
            }
        } finally {
            statsReadWriteLock.unlock();
        }
    }

    public void reportInitializationStartTs(long initializationStartTs) {
        jobInitializationMetricsBuilder =
                Optional.of(
                        new JobInitializationMetricsBuilder(
                                totalNumberOfSubTasks, initializationStartTs));
    }

    public void reportInitializationMetrics(SubTaskInitializationMetrics initializationMetrics) {
        statsReadWriteLock.lock();
        try {
            if (!jobInitializationMetricsBuilder.isPresent()) {
                LOG.warn(
                        "Attempted to report SubTaskInitializationMetrics [{}] without jobInitializationMetricsBuilder present",
                        initializationMetrics);
                return;
            }
            JobInitializationMetricsBuilder builder = jobInitializationMetricsBuilder.get();
            builder.reportInitializationMetrics(initializationMetrics);
            if (builder.isComplete()) {
                traceInitializationMetrics(builder.build());
            }
        } catch (Exception ex) {
            LOG.warn("Failed to log SubTaskInitializationMetrics[{}]", ex, initializationMetrics);
        } finally {
            statsReadWriteLock.unlock();
        }
    }

    private void traceInitializationMetrics(JobInitializationMetrics jobInitializationMetrics) {
        SpanBuilder span =
                Span.builder(CheckpointStatsTracker.class, "JobInitialization")
                        .setStartTsMillis(jobInitializationMetrics.getStartTs())
                        .setEndTsMillis(jobInitializationMetrics.getEndTs())
                        .setAttribute(
                                "initializationStatus",
                                jobInitializationMetrics.getStatus().name());
        for (JobInitializationMetrics.SumMaxDuration duration :
                jobInitializationMetrics.getDurationMetrics().values()) {
            setDurationSpanAttribute(span, duration);
        }
        if (jobInitializationMetrics.getCheckpointId() != JobInitializationMetrics.UNSET) {
            span.setAttribute("checkpointId", jobInitializationMetrics.getCheckpointId());
        }
        if (jobInitializationMetrics.getStateSize() != JobInitializationMetrics.UNSET) {
            span.setAttribute("fullSize", jobInitializationMetrics.getStateSize());
        }
        metricGroup.addSpan(span);
    }

    private void setDurationSpanAttribute(
            SpanBuilder span, JobInitializationMetrics.SumMaxDuration duration) {
        span.setAttribute("max" + duration.getName(), duration.getMax());
        span.setAttribute("sum" + duration.getName(), duration.getSum());
    }

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    @VisibleForTesting
    static final String NUMBER_OF_CHECKPOINTS_METRIC = "totalNumberOfCheckpoints";

    @VisibleForTesting
    static final String NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC = "numberOfInProgressCheckpoints";

    @VisibleForTesting
    static final String NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC = "numberOfCompletedCheckpoints";

    @VisibleForTesting
    static final String NUMBER_OF_FAILED_CHECKPOINTS_METRIC = "numberOfFailedCheckpoints";

    @VisibleForTesting
    static final String LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC =
            "lastCheckpointRestoreTimestamp";

    @VisibleForTesting
    static final String LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC = "lastCheckpointSize";

    @VisibleForTesting
    static final String LATEST_COMPLETED_CHECKPOINT_FULL_SIZE_METRIC = "lastCheckpointFullSize";

    @VisibleForTesting
    static final String LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC = "lastCheckpointDuration";

    @VisibleForTesting
    static final String LATEST_COMPLETED_CHECKPOINT_PROCESSED_DATA_METRIC =
            "lastCheckpointProcessedData";

    @VisibleForTesting
    static final String LATEST_COMPLETED_CHECKPOINT_PERSISTED_DATA_METRIC =
            "lastCheckpointPersistedData";

    @VisibleForTesting
    static final String LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC =
            "lastCheckpointExternalPath";

    @VisibleForTesting
    static final String LATEST_COMPLETED_CHECKPOINT_ID_METRIC = "lastCompletedCheckpointId";

    /**
     * Register the exposed metrics.
     *
     * @param metricGroup Metric group to use for the metrics.
     */
    private void registerMetrics(MetricGroup metricGroup) {
        metricGroup.gauge(NUMBER_OF_CHECKPOINTS_METRIC, new CheckpointsCounter());
        metricGroup.gauge(
                NUMBER_OF_IN_PROGRESS_CHECKPOINTS_METRIC, new InProgressCheckpointsCounter());
        metricGroup.gauge(
                NUMBER_OF_COMPLETED_CHECKPOINTS_METRIC, new CompletedCheckpointsCounter());
        metricGroup.gauge(NUMBER_OF_FAILED_CHECKPOINTS_METRIC, new FailedCheckpointsCounter());
        metricGroup.gauge(
                LATEST_RESTORED_CHECKPOINT_TIMESTAMP_METRIC,
                new LatestRestoredCheckpointTimestampGauge());
        metricGroup.gauge(
                LATEST_COMPLETED_CHECKPOINT_SIZE_METRIC, new LatestCompletedCheckpointSizeGauge());
        metricGroup.gauge(
                LATEST_COMPLETED_CHECKPOINT_FULL_SIZE_METRIC,
                new LatestCompletedCheckpointFullSizeGauge());
        metricGroup.gauge(
                LATEST_COMPLETED_CHECKPOINT_DURATION_METRIC,
                new LatestCompletedCheckpointDurationGauge());
        metricGroup.gauge(
                LATEST_COMPLETED_CHECKPOINT_PROCESSED_DATA_METRIC,
                new LatestCompletedCheckpointProcessedDataGauge());
        metricGroup.gauge(
                LATEST_COMPLETED_CHECKPOINT_PERSISTED_DATA_METRIC,
                new LatestCompletedCheckpointPersistedDataGauge());
        metricGroup.gauge(
                LATEST_COMPLETED_CHECKPOINT_EXTERNAL_PATH_METRIC,
                new LatestCompletedCheckpointExternalPathGauge());
        metricGroup.gauge(
                LATEST_COMPLETED_CHECKPOINT_ID_METRIC, new LatestCompletedCheckpointIdGauge());
    }

    private class CheckpointsCounter implements Gauge<Long> {
        @Override
        public Long getValue() {
            return counts.getTotalNumberOfCheckpoints();
        }
    }

    private class InProgressCheckpointsCounter implements Gauge<Integer> {
        @Override
        public Integer getValue() {
            return counts.getNumberOfInProgressCheckpoints();
        }
    }

    private class CompletedCheckpointsCounter implements Gauge<Long> {
        @Override
        public Long getValue() {
            return counts.getNumberOfCompletedCheckpoints();
        }
    }

    private class FailedCheckpointsCounter implements Gauge<Long> {
        @Override
        public Long getValue() {
            return counts.getNumberOfFailedCheckpoints();
        }
    }

    private class LatestRestoredCheckpointTimestampGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            return jobInitializationMetricsBuilder
                    .map(JobInitializationMetricsBuilder::getStartTs)
                    .orElse(-1L);
        }
    }

    private class LatestCompletedCheckpointSizeGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CompletedCheckpointStats completed = latestCompletedCheckpoint;
            if (completed != null) {
                return completed.getCheckpointedSize();
            } else {
                return -1L;
            }
        }
    }

    private class LatestCompletedCheckpointFullSizeGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CompletedCheckpointStats completed = latestCompletedCheckpoint;
            if (completed != null) {
                return completed.getStateSize();
            } else {
                return -1L;
            }
        }
    }

    private class LatestCompletedCheckpointDurationGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CompletedCheckpointStats completed = latestCompletedCheckpoint;
            if (completed != null) {
                return completed.getEndToEndDuration();
            } else {
                return -1L;
            }
        }
    }

    private class LatestCompletedCheckpointProcessedDataGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CompletedCheckpointStats completed = latestCompletedCheckpoint;
            if (completed != null) {
                return completed.getProcessedData();
            } else {
                return -1L;
            }
        }
    }

    private class LatestCompletedCheckpointPersistedDataGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CompletedCheckpointStats completed = latestCompletedCheckpoint;
            if (completed != null) {
                return completed.getPersistedData();
            } else {
                return -1L;
            }
        }
    }

    private class LatestCompletedCheckpointExternalPathGauge implements Gauge<String> {
        @Override
        public String getValue() {
            CompletedCheckpointStats completed = latestCompletedCheckpoint;
            if (completed != null && completed.getExternalPath() != null) {
                return completed.getExternalPath();
            } else {
                return "n/a";
            }
        }
    }

    private class LatestCompletedCheckpointIdGauge implements Gauge<Long> {
        @Override
        public Long getValue() {
            CompletedCheckpointStats completed = latestCompletedCheckpoint;
            if (completed != null) {
                return completed.getCheckpointId();
            } else {
                return -1L;
            }
        }
    }
}
