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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;

/**
 * An {@link Output} that allows watermark progression to yield to other mails and resume later
 * using the mailbox.
 *
 * @param <OUT> The type of the elements that can be emitted.
 */
@Internal
public class WatermarkHoldingOutput<OUT> implements Output<OUT> {

    private static final Logger LOG =
            LoggerFactory.getLogger(WatermarkHoldingOutput.class.getName());

    public static <OUT> void emitWatermarkInsideMailbox(
            AbstractStreamOperator<OUT> operator, Watermark mark) throws Exception {
        Preconditions.checkState(operator.getTimeServiceManager().isPresent());
        final InternalTimeServiceManager<?> timeServiceManager =
                operator.getTimeServiceManager().get();
        if (operator.output instanceof WatermarkHoldingOutput) {
            @SuppressWarnings("unchecked")
            final WatermarkHoldingOutput<OUT> castOutput =
                    (WatermarkHoldingOutput<OUT>) operator.output;
            castOutput.emitWatermarkInsideMailbox(mark, timeServiceManager::tryAdvanceWatermark);
        } else {
            LOG.warn(
                    "Operator {} does not support watermark holds, emitting watermark directly. This should only happen when using test harness.",
                    operator.getOperatorName());
            timeServiceManager.advanceWatermark(mark);
            operator.output.emitWatermark(mark);
        }
    }

    /**
     * An interfaces that allows progressing the watermark unless shouldStopAdvancing returns true.
     */
    @Internal
    @FunctionalInterface
    public interface AdvanceWatermarkFn {

        /**
         * Try to fully advance the watermark, unless shouldStopAdvancing prevents us from doing so.
         *
         * @param mark The watermark to advance to.
         * @param shouldStopAdvancingFn A function that returns true if we should stop advancing the
         *     watermark.
         * @return true If the watermark has been fully advanced, false otherwise.
         * @throws Exception If advancing the watermark fails.
         */
        boolean apply(
                Watermark mark,
                InternalTimeServiceManager.ShouldStopAdvancingFn shouldStopAdvancingFn)
                throws Exception;
    }

    private final Output<OUT> delegate;
    private final MailboxExecutor taskMailboxExecutor;
    private final InternalTimeServiceManager.ShouldStopAdvancingFn shouldStopAdvancingFn;

    /**
     * Flag to indicate whether a progress watermark is scheduled in the mailbox. This is used to
     * avoid duplicate scheduling in case we have multiple watermarks to process.
     */
    private boolean progressWatermarkScheduled = false;

    /** Sorted queue of watermarks scheduled for processing. */
    private final Queue<Long> inputWatermarks = new LinkedList<>();

    public WatermarkHoldingOutput(
            Output<OUT> delegate,
            MailboxExecutor taskMailboxExecutor,
            InternalTimeServiceManager.ShouldStopAdvancingFn shouldStopAdvancingFn) {
        this.delegate = delegate;
        this.taskMailboxExecutor = taskMailboxExecutor;
        this.shouldStopAdvancingFn = shouldStopAdvancingFn;
    }

    @Override
    public void collect(OUT record) {
        delegate.collect(record);
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void emitWatermark(Watermark mark) {
        delegate.emitWatermark(mark);
    }

    public void emitWatermarkInsideMailbox(Watermark mark, AdvanceWatermarkFn advanceWatermark)
            throws Exception {
        if (inputWatermarks.isEmpty() || mark.getTimestamp() > inputWatermarks.peek()) {
            inputWatermarks.add(mark.getTimestamp());
        }
        final long minInputWatermark = Objects.requireNonNull(inputWatermarks.peek());
        // Try to progress min watermark as far as we can.
        if (advanceWatermark.apply(new Watermark(minInputWatermark), shouldStopAdvancingFn)) {
            // In case output watermark has fully progressed, remove it from the queue and emit it
            // downstream.
            Preconditions.checkArgument(
                    Objects.requireNonNull(inputWatermarks.poll()) == minInputWatermark);
            delegate.emitWatermark(new Watermark(minInputWatermark));
            if (!inputWatermarks.isEmpty()) {
                // If we have more watermarks to process, try to process them immediately.
                emitWatermarkInsideMailbox(new Watermark(inputWatermarks.peek()), advanceWatermark);
            }
        } else if (!progressWatermarkScheduled) {
            progressWatermarkScheduled = true;
            // We still have work to do, but we need to let other mails to be processed first.
            taskMailboxExecutor.execute(
                    () -> {
                        progressWatermarkScheduled = false;
                        emitWatermarkInsideMailbox(
                                new Watermark(minInputWatermark), advanceWatermark);
                    },
                    "progressWatermark");
        } else {
            // We're not guaranteed that MailboxProcessor is going to process all mails before
            // processing additional input, so the advanceWatermark could be called before the
            // previous watermark is fully processed.
            LOG.debug("Progress watermark is already scheduled, skipping.");
        }
    }

    @Override
    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
        delegate.emitWatermarkStatus(watermarkStatus);
    }

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
        delegate.collect(outputTag, record);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {
        delegate.emitLatencyMarker(latencyMarker);
    }
}
