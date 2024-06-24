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

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.ttl.mock.MockStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.configuration.ExecutionOptions.TASK_INITIALIZATION_TIMEOUT;
import static org.apache.flink.configuration.JobManagerOptions.SCHEDULER;
import static org.apache.flink.util.Preconditions.checkState;

/** Test abortion the recovery when it takes too long. */
@ExtendWith({TestLoggerExtension.class, ParameterizedTestExtension.class})
public class JobRecoveryTimeoutTest {
    private static final Logger LOG = LoggerFactory.getLogger(JobRecoveryTimeoutTest.class);

    @RegisterExtension
    static final SharedObjectsExtension SHARED_OBJECTS = SharedObjectsExtension.create();

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @NotNull
    private static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(SCHEDULER, SchedulerType.Adaptive);
        configuration.set(TASK_INITIALIZATION_TIMEOUT, Duration.ofMillis(200));
        return configuration;
    }

    @Test
    void testStateRecoveryTimeout() throws Exception {
        SharedReference<AtomicBoolean> wasBlocked = SHARED_OBJECTS.add(new AtomicBoolean(false));
        SharedReference<AtomicBoolean> stateBackendCreated =
                SHARED_OBJECTS.add(new AtomicBoolean(false));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setMaxParallelism(1);
        // Normally only single restart is expected. However if test is slow to execute (e.g. when
        // testing on small machines), the recovery timeout can be exceeded in 2nd or following
        // recovery attempts as well. For this reason we allow indefinite restarts.
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.milliseconds(10)));
        env.setStateBackend(new BlockingStateBackend(wasBlocked, stateBackendCreated));
        env.fromElements(1, 2, 3).keyBy(value -> 0).addSink(new DiscardingSink<>());
        env.execute();
        checkState(wasBlocked.get().get(), "state backend didn't block, test setup must be wrong");
        Assertions.assertTrue(stateBackendCreated.get().get(), "state backend didn't unblock");
    }

    private static class BlockingStateBackend extends MockStateBackend implements StateBackend {
        private final SharedReference<AtomicBoolean> wasBlocked;
        private final SharedReference<AtomicBoolean> wasCreated;

        public BlockingStateBackend(
                SharedReference<AtomicBoolean> wasBlocked,
                SharedReference<AtomicBoolean> wasCreated) {
            this.wasBlocked = wasBlocked;
            this.wasCreated = wasCreated;
        }

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) {
            if (wasBlocked.get().compareAndSet(false, true)) {
                LOG.debug("Blocking on state backend creation");
                block();
                throw new AssertionError("Shouldn't happen");
            } else {
                LOG.debug("Proceeding with state backend creation");
                wasCreated.get().set(true);
                return super.createKeyedStateBackend(parameters);
            }
        }

        @SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
        private static void block() {
            while (true) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    // JM will interrupt this task - don't ignore it to free the slot faster
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
