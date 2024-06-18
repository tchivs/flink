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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.AttributeBuilder;
import org.apache.flink.events.Events;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.traces.Span;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** Helper class to simplify job failure reporting through a metric group. */
public class JobFailureMetricReporter {

    public static final String FAILURE_LABEL_ATTRIBUTE_PREFIX = "failureLabel.";
    public static final String RESTART_KEY = "canRestart";
    public static final String GLOBAL_KEY = "isGlobalFailure";

    private final MetricGroup metricGroup;

    private final boolean reportAsFailureAsSpan;

    public JobFailureMetricReporter(MetricGroup metricGroup, boolean reportAsFailureAsSpan) {
        this.metricGroup = Preconditions.checkNotNull(metricGroup);
        this.reportAsFailureAsSpan = reportAsFailureAsSpan;
    }

    public void reportJobFailure(
            FailureHandlingResult failureHandlingResult, Map<String, String> failureLabels) {
        reportJobFailure(
                failureHandlingResult.getTimestamp(),
                failureHandlingResult.canRestart(),
                failureHandlingResult.isGlobalFailure(),
                failureLabels);
    }

    public void reportJobFailure(
            FailureResult failureHandlingResult, Map<String, String> failureLabels) {
        reportJobFailure(
                System.currentTimeMillis(),
                failureHandlingResult.canRestart(),
                null,
                failureLabels);
    }

    private void reportJobFailure(
            long timestamp,
            Boolean canRestart,
            Boolean isGlobal,
            Map<String, String> failureLabels) {
        if (reportAsFailureAsSpan) {
            reportJobFailureAsSpan(timestamp, canRestart, isGlobal, failureLabels);
        } else {
            reportJobFailureAsEvent(timestamp, canRestart, isGlobal, failureLabels);
        }
    }

    private void reportJobFailureAsSpan(
            long timestamp,
            Boolean canRestart,
            Boolean isGlobal,
            Map<String, String> failureLabels) {
        metricGroup.addSpan(
                addFailureAttributes(
                        Span.builder(JobFailureMetricReporter.class, Events.JobFailure.name())
                                .setStartTsMillis(timestamp)
                                .setEndTsMillis(timestamp),
                        canRestart,
                        isGlobal,
                        failureLabels));
    }

    private void reportJobFailureAsEvent(
            long timestamp,
            Boolean canRestart,
            Boolean isGlobal,
            Map<String, String> failureLabels) {
        metricGroup.addEvent(
                addFailureAttributes(
                        Events.JobFailure.builder(JobFailureMetricReporter.class)
                                .setObservedTsMillis(timestamp)
                                .setSeverity("INFO"),
                        canRestart,
                        isGlobal,
                        failureLabels));
    }

    private <T extends AttributeBuilder> T addFailureAttributes(
            T attributeBuilder,
            Boolean canRestart,
            Boolean isGlobal,
            Map<String, String> failureLabels) {
        if (canRestart != null) {
            attributeBuilder.setAttribute(RESTART_KEY, String.valueOf(canRestart));
        }

        if (isGlobal != null) {
            attributeBuilder.setAttribute(GLOBAL_KEY, String.valueOf(isGlobal));
        }

        // Add all failure labels
        for (Map.Entry<String, String> entry : failureLabels.entrySet()) {
            attributeBuilder.setAttribute(
                    FAILURE_LABEL_ATTRIBUTE_PREFIX + entry.getKey(), entry.getValue());
        }

        return attributeBuilder;
    }
}
