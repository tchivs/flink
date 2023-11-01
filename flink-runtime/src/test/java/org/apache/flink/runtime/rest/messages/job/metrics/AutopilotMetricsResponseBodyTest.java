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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

import org.junit.Test;

import java.text.ParseException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AutopilotMetricsResponseBody}. */
public class AutopilotMetricsResponseBodyTest
        extends RestResponseMarshallingTestBase<AutopilotMetricsResponseBody> {

    private static final QueryScopeInfo.TaskManagerQueryScopeInfo queryScopeInfo =
            new QueryScopeInfo.TaskManagerQueryScopeInfo("tm1");

    @Override
    protected Class<AutopilotMetricsResponseBody> getTestResponseClass() {
        return AutopilotMetricsResponseBody.class;
    }

    @Override
    protected AutopilotMetricsResponseBody getTestResponseInstance() throws Exception {
        final Map<String, Instant> taskManagerTimestamps = new HashMap<>();
        final Map<String, Map<String, Number>> taskManagerMetrics = new HashMap<>();
        final Map<String, Number> taskManagerCounters = new HashMap<>();
        taskManagerTimestamps.put("counters", Instant.now());
        taskManagerCounters.put("foo", 1);
        taskManagerCounters.put("bar", 2);
        taskManagerMetrics.putIfAbsent("counters", taskManagerCounters);
        return new AutopilotMetricsResponseBody(
                taskManagerTimestamps,
                taskManagerMetrics,
                Collections.emptyMap(),
                Collections.emptyMap());
    }

    @Test
    public void testSpecialGaugeDoubleValuesOmitted() throws ParseException {
        final AutopilotMetricsResponseBody response = new AutopilotMetricsResponseBody();

        response.addMetric(
                new MetricDump.GaugeDump(
                        queryScopeInfo,
                        "infinity-positive",
                        Double.toString(Double.POSITIVE_INFINITY)),
                Instant.now());
        response.addMetric(
                new MetricDump.GaugeDump(
                        queryScopeInfo,
                        "infinity-negative",
                        Double.toString(Double.NEGATIVE_INFINITY)),
                Instant.now());

        response.addMetric(
                new MetricDump.GaugeDump(queryScopeInfo, "NaN", Double.toString(Double.NaN)),
                Instant.now());

        assertThat(response.getTaskManagerMetrics())
                .hasEntrySatisfying(
                        queryScopeInfo.taskManagerID, metrics -> assertThat(metrics).isEmpty());
    }

    @Test
    public void testSpecialMeterDoubleValuesOmitted() throws ParseException {
        final AutopilotMetricsResponseBody response = new AutopilotMetricsResponseBody();

        response.addMetric(
                new MetricDump.MeterDump(
                        queryScopeInfo, "infinity-positive", Double.POSITIVE_INFINITY),
                Instant.now());
        response.addMetric(
                new MetricDump.MeterDump(
                        queryScopeInfo, "infinity-negative", Double.NEGATIVE_INFINITY),
                Instant.now());

        response.addMetric(
                new MetricDump.MeterDump(queryScopeInfo, "NaN", Double.NaN), Instant.now());

        assertThat(response.getTaskManagerMetrics())
                .hasEntrySatisfying(
                        queryScopeInfo.taskManagerID, metrics -> assertThat(metrics).isEmpty());
    }

    @Test
    public void testTimestampSetForTaskManagerMetrics() throws ParseException {
        final AutopilotMetricsResponseBody response = new AutopilotMetricsResponseBody();

        final Instant timestamp = Instant.now();
        final QueryScopeInfo.JobQueryScopeInfo jobScopeInfo =
                new QueryScopeInfo.JobQueryScopeInfo("jobid");
        final QueryScopeInfo.TaskManagerQueryScopeInfo taskManagerQueryScopeInfo = queryScopeInfo;

        response.addMetric(new MetricDump.MeterDump(jobScopeInfo, "jobMetric", 0), timestamp);

        assertThat(response.getTaskManagerTimestamps()).isEmpty();

        response.addMetric(
                new MetricDump.MeterDump(taskManagerQueryScopeInfo, "taskManagerMetric", 0),
                timestamp);

        assertThat(response.getTaskManagerTimestamps())
                .containsEntry(taskManagerQueryScopeInfo.taskManagerID, timestamp);
    }
}
