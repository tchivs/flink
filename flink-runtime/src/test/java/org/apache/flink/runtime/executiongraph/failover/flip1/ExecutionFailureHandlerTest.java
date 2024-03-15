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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.TestingFailureEnricher;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptive.JobFailureMetricReporter;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.util.IterableUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResultTest.createExecution;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExecutionFailureHandler}. */
class ExecutionFailureHandlerTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final long RESTART_DELAY_MS = 1234L;

    private SchedulingTopology schedulingTopology;

    private TestFailoverStrategy failoverStrategy;

    private TestRestartBackoffTimeStrategy backoffTimeStrategy;

    private ExecutionFailureHandler executionFailureHandler;

    private TestingFailureEnricher testingFailureEnricher;

    private List<Span> spanCollector;

    @BeforeEach
    void setUp() {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();
        topology.newExecutionVertex();
        schedulingTopology = topology;

        failoverStrategy = new TestFailoverStrategy();
        testingFailureEnricher = new TestingFailureEnricher();
        backoffTimeStrategy = new TestRestartBackoffTimeStrategy(true, RESTART_DELAY_MS);
        spanCollector = new CopyOnWriteArrayList<>();
        Configuration configuration = new Configuration();
        configuration.set(TraceOptions.REPORT_EVENTS_AS_SPANS, Boolean.TRUE);
        executionFailureHandler =
                new ExecutionFailureHandler(
                        configuration,
                        schedulingTopology,
                        failoverStrategy,
                        backoffTimeStrategy,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        Collections.singleton(testingFailureEnricher),
                        null,
                        null,
                        new UnregisteredMetricsGroup() {
                            @Override
                            public void addSpan(SpanBuilder spanBuilder) {
                                spanCollector.add(spanBuilder.build());
                            }
                        });
    }

    /** Tests the case that task restarting is accepted. */
    @Test
    void testNormalFailureHandling() throws Exception {
        final Set<ExecutionVertexID> tasksToRestart =
                Collections.singleton(new ExecutionVertexID(new JobVertexID(), 0));
        failoverStrategy.setTasksToRestart(tasksToRestart);

        Execution execution = createExecution(EXECUTOR_RESOURCE.getExecutor());
        Exception cause = new Exception("test failure");
        long timestamp = System.currentTimeMillis();
        // trigger a task failure
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(execution, cause, timestamp);

        // verify results
        assertThat(result.canRestart()).isTrue();
        assertThat(result.getFailedExecution()).isPresent();
        assertThat(result.getFailedExecution().get()).isSameAs(execution);
        assertThat(result.getRestartDelayMS()).isEqualTo(RESTART_DELAY_MS);
        assertThat(result.getVerticesToRestart()).isEqualTo(tasksToRestart);
        assertThat(result.getError()).isSameAs(cause);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(cause);
        assertThat(result.getFailureLabels().get())
                .isEqualTo(testingFailureEnricher.getFailureLabels());
        assertThat(executionFailureHandler.getNumberOfRestarts()).isEqualTo(1);
        checkMetrics(spanCollector, false, true);
    }

    @Test
    void testLabeling() throws Exception {
        final Map<String, String> expectedFailureLabels =
                Collections.singletonMap(FailureEnricher.KEY_JOB_CANNOT_RESTART, "");

        testingFailureEnricher.setFailureLabels(expectedFailureLabels);
        testingFailureEnricher.setOutputKeys(expectedFailureLabels.keySet());

        final Set<ExecutionVertexID> tasksToRestart =
                Collections.singleton(new ExecutionVertexID(new JobVertexID(), 0));
        failoverStrategy.setTasksToRestart(tasksToRestart);

        Execution execution = createExecution(EXECUTOR_RESOURCE.getExecutor());
        Exception cause = new Exception("test failure");
        long timestamp = System.currentTimeMillis();
        // trigger a task failure
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(execution, cause, timestamp);

        // verify results
        assertThat(result.canRestart()).isFalse();
        assertThat(result.getFailedExecution()).isPresent();
        assertThat(result.getFailedExecution().get()).isSameAs(execution);
        assertThat(result.getError()).hasCause(cause);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(cause);
        assertThat(result.getFailureLabels().get()).isEqualTo(expectedFailureLabels);
        assertThat(executionFailureHandler.getNumberOfRestarts()).isZero();
    }

    /** Tests the case that task restarting is suppressed. */
    @Test
    void testRestartingSuppressedFailureHandlingResult() throws Exception {
        // restart strategy suppresses restarting
        backoffTimeStrategy.setCanRestart(false);

        // trigger a task failure
        Execution execution = createExecution(EXECUTOR_RESOURCE.getExecutor());
        final Throwable error = new Exception("expected test failure");
        final long timestamp = System.currentTimeMillis();
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(execution, error, timestamp);

        // verify results
        assertThat(result.canRestart()).isFalse();
        assertThat(result.getFailedExecution()).isPresent();
        assertThat(result.getFailedExecution().get()).isSameAs(execution);
        assertThat(result.getError()).hasCause(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(error);
        assertThat(result.getFailureLabels().get())
                .isEqualTo(testingFailureEnricher.getFailureLabels());
        assertThat(
                        ExecutionFailureHandler.isUnrecoverableError(
                                result.getError(),
                                CompletableFuture.completedFuture(Collections.emptyMap())))
                .isFalse();

        assertThatThrownBy(result::getVerticesToRestart)
                .as("getVerticesToRestart is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(result::getRestartDelayMS)
                .as("getRestartDelayMS is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThat(executionFailureHandler.getNumberOfRestarts()).isZero();
        checkMetrics(spanCollector, false, false);
    }

    /** Tests the case that the failure is non-recoverable type. */
    @Test
    void testNonRecoverableFailureHandlingResult() throws Exception {

        // trigger an unrecoverable task failure
        Execution execution = createExecution(EXECUTOR_RESOURCE.getExecutor());
        final Throwable error =
                new Exception(new SuppressRestartsException(new Exception("test failure")));
        final long timestamp = System.currentTimeMillis();
        final FailureHandlingResult result =
                executionFailureHandler.getFailureHandlingResult(execution, error, timestamp);

        // verify results
        assertThat(result.canRestart()).isFalse();
        assertThat(result.getFailedExecution()).isPresent();
        assertThat(result.getFailedExecution().get()).isSameAs(execution);
        assertThat(result.getError()).isNotNull();
        assertThat(
                        ExecutionFailureHandler.isUnrecoverableError(
                                result.getError(),
                                CompletableFuture.completedFuture(Collections.emptyMap())))
                .isTrue();
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(error);
        assertThat(result.getFailureLabels().get())
                .isEqualTo(testingFailureEnricher.getFailureLabels());
        assertThat(result.getTimestamp()).isEqualTo(timestamp);

        assertThatThrownBy(result::getVerticesToRestart)
                .as("getVerticesToRestart is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(result::getRestartDelayMS)
                .as("getRestartDelayMS is not allowed when restarting is suppressed")
                .isInstanceOf(IllegalStateException.class);

        assertThat(executionFailureHandler.getNumberOfRestarts()).isZero();
        checkMetrics(spanCollector, false, false);
    }

    /** Tests the check for unrecoverable error. */
    @Test
    void testUnrecoverableErrorCheck() {
        // normal error
        assertThat(
                        ExecutionFailureHandler.isUnrecoverableError(
                                new Exception(),
                                CompletableFuture.completedFuture(Collections.emptyMap())))
                .isFalse();

        // direct unrecoverable error
        assertThat(
                        ExecutionFailureHandler.isUnrecoverableError(
                                new SuppressRestartsException(new Exception()),
                                CompletableFuture.completedFuture(Collections.emptyMap())))
                .isTrue();

        // nested unrecoverable error
        assertThat(
                        ExecutionFailureHandler.isUnrecoverableError(
                                new Exception(new SuppressRestartsException(new Exception())),
                                CompletableFuture.completedFuture(Collections.emptyMap())))
                .isTrue();
    }

    @Test
    void testGlobalFailureHandling() throws ExecutionException, InterruptedException {
        final Throwable error = new Exception("Expected test failure");
        final long timestamp = System.currentTimeMillis();
        final FailureHandlingResult result =
                executionFailureHandler.getGlobalFailureHandlingResult(error, timestamp);

        assertThat(result.getVerticesToRestart())
                .isEqualTo(
                        IterableUtils.toStream(schedulingTopology.getVertices())
                                .map(SchedulingExecutionVertex::getId)
                                .collect(Collectors.toSet()));
        assertThat(result.getError()).isSameAs(error);
        assertThat(result.getTimestamp()).isEqualTo(timestamp);
        assertThat(testingFailureEnricher.getSeenThrowables()).containsExactly(error);
        assertThat(result.getFailureLabels().get())
                .isEqualTo(testingFailureEnricher.getFailureLabels());
        checkMetrics(spanCollector, true, true);
    }

    // ------------------------------------------------------------------------
    //  utilities
    // ------------------------------------------------------------------------

    /**
     * A FailoverStrategy implementation for tests. It always suggests restarting the given tasks to
     * restart.
     */
    private static class TestFailoverStrategy implements FailoverStrategy {

        private Set<ExecutionVertexID> tasksToRestart;

        public TestFailoverStrategy() {}

        public void setTasksToRestart(final Set<ExecutionVertexID> tasksToRestart) {
            this.tasksToRestart = tasksToRestart;
        }

        @Override
        public Set<ExecutionVertexID> getTasksNeedingRestart(
                final ExecutionVertexID executionVertexId, final Throwable cause) {

            return tasksToRestart;
        }
    }

    private void checkMetrics(List<Span> results, boolean global, boolean canRestart) {
        assertThat(results).isNotEmpty();
        for (Span span : results) {
            assertThat(span.getScope())
                    .isEqualTo(JobFailureMetricReporter.class.getCanonicalName());
            assertThat(span.getName()).isEqualTo("JobFailure");
            Map<String, Object> attributes = span.getAttributes();
            assertThat(attributes).containsEntry("failureLabel.failKey", "failValue");
            assertThat(attributes).containsEntry("canRestart", String.valueOf(canRestart));
            assertThat(attributes).containsEntry("isGlobalFailure", String.valueOf(global));
        }
    }
}
