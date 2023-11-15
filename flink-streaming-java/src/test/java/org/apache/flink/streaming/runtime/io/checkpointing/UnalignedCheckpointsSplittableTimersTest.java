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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.WatermarkHoldingOutput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamTaskMailboxTestHarnessBuilder;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests interaction between {@link WatermarkHoldingOutput} and unaligned checkpoints. */
@ExtendWith(TestLoggerExtension.class)
class UnalignedCheckpointsSplittableTimersTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static Watermark asWatermark(Instant timestamp) {
        return new Watermark(timestamp.toEpochMilli());
    }

    private static void setupStreamConfig(StreamConfig cfg) {
        cfg.setUnalignedCheckpointsEnabled(true);
        cfg.setUnalignedCheckpointsSplittableTimersEnabled(true);
        cfg.setStateKeySerializer(StringSerializer.INSTANCE);
    }

    private static class MultipleTimersAtTheSameTimestamp extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String>, Triggerable<String, String> {

        private static final ConcurrentMap<String, CountDownLatch> FIRED_LATCHES =
                new ConcurrentHashMap<>();
        private static final ConcurrentMap<String, CountDownLatch> PROCESSED_LATCHES =
                new ConcurrentHashMap<>();

        private final String testId;
        private final Map<Instant, Integer> timersToRegister;
        private final boolean blockingTimers;

        static void awaitOnTimer(
                String testId, CountDownLatch timerFired, CountDownLatch timerProcessed) {
            FIRED_LATCHES.put(testId, timerFired);
            PROCESSED_LATCHES.put(testId, timerProcessed);
        }

        static void clear(String testId) {
            FIRED_LATCHES.remove(testId);
            PROCESSED_LATCHES.remove(testId);
        }

        MultipleTimersAtTheSameTimestamp(String testId) {
            this(testId, Collections.emptyMap(), false);
        }

        MultipleTimersAtTheSameTimestamp(
                String testId, Map<Instant, Integer> timersToRegister, boolean blockingTimers) {
            this.testId = testId;
            this.timersToRegister = timersToRegister;
            this.blockingTimers = blockingTimers;
        }

        @Override
        public void processElement(StreamRecord<String> element) {
            if (!timersToRegister.isEmpty()) {
                final InternalTimerService<String> timers =
                        getInternalTimerService("timers", StringSerializer.INSTANCE, this);
                for (Map.Entry<Instant, Integer> entry : timersToRegister.entrySet()) {
                    for (int keyIdx = 0; keyIdx < entry.getValue(); keyIdx++) {
                        final String key = String.format("key-%d", keyIdx);
                        setCurrentKey(key);
                        timers.registerEventTimeTimer(
                                String.format("window-%s", entry.getKey()),
                                entry.getKey().toEpochMilli());
                    }
                }
            }
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            WatermarkHoldingOutput.emitWatermarkInsideMailbox(this, mark);
        }

        @Override
        public void onEventTime(InternalTimer<String, String> timer) throws Exception {
            if (blockingTimers) {
                Objects.requireNonNull(FIRED_LATCHES.get(testId)).countDown();
                Objects.requireNonNull(PROCESSED_LATCHES.get(testId)).await();
            }
            output.collect(new StreamRecord<>("fired-" + timer.getKey()));
        }

        @Override
        public void onProcessingTime(InternalTimer<String, String> timer) throws Exception {}

        MultipleTimersAtTheSameTimestamp withTimers(Instant timestamp, int count) {
            final Map<Instant, Integer> copy = new HashMap<>(timersToRegister);
            copy.put(timestamp, count);
            return new MultipleTimersAtTheSameTimestamp(testId, copy, blockingTimers);
        }

        MultipleTimersAtTheSameTimestamp withBlockingTimers() {
            return new MultipleTimersAtTheSameTimestamp(testId, timersToRegister, true);
        }
    }

    @Test
    void testSingleWatermarkHoldingOperatorInTheChain() throws Exception {
        final String testId = UUID.randomUUID().toString();
        final Instant firstWindowEnd = Instant.ofEpochMilli(1000L);
        final int numFirstWindowTimers = 123;
        final Instant secondWindowEnd = Instant.ofEpochMilli(2000L);
        final int numSecondWindowTimers = 321;

        try (final StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(OneInputStreamTask::new, Types.STRING)
                        .modifyStreamConfig(
                                UnalignedCheckpointsSplittableTimersTest::setupStreamConfig)
                        .addInput(Types.STRING)
                        .setupOperatorChain(
                                SimpleOperatorFactory.of(
                                        new MultipleTimersAtTheSameTimestamp(testId)
                                                .withBlockingTimers()
                                                .withTimers(firstWindowEnd, numFirstWindowTimers)
                                                .withTimers(
                                                        secondWindowEnd, numSecondWindowTimers)))
                        .name("first")
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build()) {
            harness.setAutoProcess(false);
            harness.processElement(new StreamRecord<>("impulse"));
            harness.processAll();
            harness.processElement(asWatermark(firstWindowEnd));
            harness.processElement(asWatermark(secondWindowEnd));

            int seenRecords = 0;
            final List<Watermark> seenWatermarks = new ArrayList<>();
            while (seenRecords < numFirstWindowTimers + numSecondWindowTimers) {
                final CountDownLatch timerFired = new CountDownLatch(1);
                final CountDownLatch timerProcessed = new CountDownLatch(1);
                final CountDownLatch barrierProcessed = new CountDownLatch(1);
                MultipleTimersAtTheSameTimestamp.awaitOnTimer(testId, timerFired, timerProcessed);

                // Once timer fires, we schedule processing of the barrier onto the main mailbox
                // executor. This simulates unaligned checkpoint after single event time timer.
                EXECUTOR_RESOURCE
                        .getExecutor()
                        .execute(
                                () -> {
                                    try {
                                        timerFired.await();
                                        harness.getMainMailboxExecutor()
                                                .execute(
                                                        barrierProcessed::countDown,
                                                        "processBarrier");
                                        timerProcessed.countDown();
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                });

                while (barrierProcessed.getCount() > 0) {
                    harness.processSingleMailboxLoop();
                }

                int recordsInThisLoop = 0;
                Object outputElement;
                while ((outputElement = harness.getOutput().poll()) != null) {
                    if (outputElement instanceof StreamRecord) {
                        recordsInThisLoop++;
                        seenRecords++;
                    }
                    if (outputElement instanceof Watermark) {
                        seenWatermarks.add((Watermark) outputElement);
                    }
                }

                assertThat(recordsInThisLoop)
                        .withFailMessage("More than one timer has been fired in a loop.")
                        .isEqualTo(1);
            }
            assertThat(seenWatermarks)
                    .containsExactly(asWatermark(firstWindowEnd), asWatermark(secondWindowEnd));
        } finally {
            MultipleTimersAtTheSameTimestamp.clear(testId);
        }
    }

    @Test
    void testWatermarkProgressWithNoTimers() throws Exception {
        final Instant firstWindowEnd = Instant.ofEpochMilli(1000L);
        final Instant secondWindowEnd = Instant.ofEpochMilli(2000L);

        try (final StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(OneInputStreamTask::new, Types.STRING)
                        .modifyStreamConfig(
                                UnalignedCheckpointsSplittableTimersTest::setupStreamConfig)
                        .addInput(Types.STRING)
                        .setupOperatorChain(
                                SimpleOperatorFactory.of(
                                        new MultipleTimersAtTheSameTimestamp(
                                                UUID.randomUUID().toString())))
                        .name("first")
                        .finishForSingletonOperatorChain(StringSerializer.INSTANCE)
                        .build()) {
            harness.setAutoProcess(false);
            harness.processElement(new StreamRecord<>("impulse"));
            harness.processAll();
            harness.processElement(asWatermark(firstWindowEnd));
            harness.processElement(asWatermark(secondWindowEnd));

            final List<Watermark> seenWatermarks = new ArrayList<>();
            while (seenWatermarks.size() < 2) {
                harness.processSingleStep();
                Object outputElement;
                while ((outputElement = harness.getOutput().poll()) != null) {
                    if (outputElement instanceof Watermark) {
                        seenWatermarks.add((Watermark) outputElement);
                    }
                }
            }
            assertThat(seenWatermarks)
                    .containsExactly(asWatermark(firstWindowEnd), asWatermark(secondWindowEnd));
        }
    }
}
