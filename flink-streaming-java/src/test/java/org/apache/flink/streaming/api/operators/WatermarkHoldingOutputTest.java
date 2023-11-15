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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.streaming.util.CollectorOutput;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WatermarkHoldingOutput}. */
class WatermarkHoldingOutputTest {

    @Test
    void testEmitWatermarkInsideMailbox() throws Exception {
        final List<StreamElement> emittedElements = new ArrayList<>();
        final TaskMailboxImpl mailbox = new TaskMailboxImpl();
        final AtomicBoolean shouldStopAdvancing = new AtomicBoolean(false);

        final WatermarkHoldingOutput<StreamRecord<String>> watermarkHoldingOutput =
                new WatermarkHoldingOutput<>(
                        new CollectorOutput<>(emittedElements),
                        new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE),
                        shouldStopAdvancing::get);

        final List<Watermark> inputWatermarks =
                LongStream.range(0, 10).mapToObj(Watermark::new).collect(Collectors.toList());

        for (Watermark intputWatermark : inputWatermarks) {
            watermarkHoldingOutput.emitWatermarkInsideMailbox(
                    intputWatermark,
                    (mark, shouldStopAdvancingFn) -> shouldStopAdvancingFn.shouldStopAdvancing());
        }

        assertThat(emittedElements).isEmpty();
        assertThat(mailbox.size()).isEqualTo(1);

        shouldStopAdvancing.set(true);
        mailbox.take(TaskMailbox.MIN_PRIORITY).run();
        assertThat(emittedElements).containsExactlyElementsOf(inputWatermarks);
    }
}
