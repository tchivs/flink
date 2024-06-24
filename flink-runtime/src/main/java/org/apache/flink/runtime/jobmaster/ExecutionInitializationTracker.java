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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.execution.ExecutionState.INITIALIZING;

/**
 * Tracks the {@link org.apache.flink.runtime.execution.ExecutionState#INITIALIZING initialization}
 * time of {@link Execution Executions} and {@link
 * org.apache.flink.runtime.executiongraph.Execution#fail fails} them upon a timeout. This prevents
 * holding any resources (such as transactions in external systems) for too long during recovery.
 */
@Internal
public class ExecutionInitializationTracker implements ExecutionStateUpdateListener {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionInitializationTracker.class);

    private final Duration timeout;
    private final ComponentMainThreadExecutor mainThreadExecutor;
    private final Map<ExecutionAttemptID, ScheduledFuture<?>> timeoutFutures;
    private final FatalErrorHandler fatalErrorHandler;

    public ExecutionInitializationTracker(
            Duration timeout,
            ComponentMainThreadExecutor mainThreadExecutor,
            FatalErrorHandler fatalErrorHandler) {
        this.timeout = timeout;
        this.mainThreadExecutor = mainThreadExecutor;
        this.timeoutFutures = new HashMap<>();
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public void onStateUpdate(
            Execution execution, ExecutionState previousState, ExecutionState newState) {

        if (newState == INITIALIZING) {
            LOG.debug("Schedule initialization timeout");
            ScheduledFuture<?> future = scheduleTimeout(execution);
            ScheduledFuture<?> prev = timeoutFutures.put(execution.getAttemptId(), future);
            // shouldn't happen given the current state transitions but might happen in the future
            cancelIfNeeded(prev, "Timeout future was already scheduled, trying to cancel", true);
        } else {
            cancelIfNeeded(
                    timeoutFutures.remove(execution.getAttemptId()),
                    "Cancel initialization timeout",
                    false);
        }
    }

    private static void cancelIfNeeded(ScheduledFuture<?> future, String log, boolean warn) {
        if (future == null) {
            return;
        }
        if (warn) {
            LOG.warn(log);
        } else {
            LOG.debug(log);
        }
        if (!future.cancel(false)) {
            LOG.warn("Unable to cancel timeout future");
        }
    }

    private ScheduledFuture<?> scheduleTimeout(Execution execution) {
        return mainThreadExecutor.schedule(
                () -> {
                    try {
                        if (execution.getState() == INITIALIZING) {
                            execution.fail(
                                    new TimeoutException(
                                            "Task initialization timed out after " + timeout));
                        }
                    } catch (Exception e) {
                        fatalErrorHandler.onFatalError(e);
                    }
                },
                timeout.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public static ExecutionStateUpdateListener forConfiguration(
            Configuration configuration,
            ComponentMainThreadExecutor mainThreadExecutor,
            FatalErrorHandler fatalErrorHandler) {
        return configuration
                .getOptional(ExecutionOptions.TASK_INITIALIZATION_TIMEOUT)
                .<ExecutionStateUpdateListener>map(
                        timeout ->
                                new ExecutionInitializationTracker(
                                        timeout, mainThreadExecutor, fatalErrorHandler))
                .orElse(ExecutionStateUpdateListener.NO_OP);
    }
}
