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

package org.apache.flink.runtime.metrics.dump;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;
import org.apache.flink.metrics.util.TestMeter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.TestingRpcServiceExtension;
import org.apache.flink.runtime.webmonitor.retriever.JobMetricsFilter;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the {@link MetricQueryService}. */
public class MetricQueryServiceTest extends TestLogger {

    private static final Time TIMEOUT = Time.hours(1);

    @RegisterExtension
    private final EachCallbackWrapper<TestingRpcServiceExtension> rpcServiceExtension =
            new EachCallbackWrapper<>(new TestingRpcServiceExtension());

    @Test
    public void testCreateDump() throws Exception {
        final MetricQueryService queryService = createAndStartMetricQueryService(Long.MAX_VALUE);

        final Counter c = new SimpleCounter();
        final Gauge<String> g = () -> "Hello";
        final Histogram h = new TestHistogram();
        final Meter m = new TestMeter();

        final TaskManagerMetricGroup tm =
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

        queryService.addMetric("counter", c, tm);
        queryService.addMetric("gauge", g, tm);
        queryService.addMetric("histogram", h, tm);
        queryService.addMetric("meter", m, tm);

        MetricDumpSerialization.MetricSerializationResult dump =
                queryService.queryMetrics(TIMEOUT).get();

        assertTrue(dump.serializedCounters.length > 0);
        assertTrue(dump.serializedGauges.length > 0);
        assertTrue(dump.serializedHistograms.length > 0);
        assertTrue(dump.serializedMeters.length > 0);

        queryService.removeMetric(c);
        queryService.removeMetric(g);
        queryService.removeMetric(h);
        queryService.removeMetric(m);

        MetricDumpSerialization.MetricSerializationResult emptyDump =
                queryService.queryMetrics(TIMEOUT).get();

        assertEquals(0, emptyDump.serializedCounters.length);
        assertEquals(0, emptyDump.serializedGauges.length);
        assertEquals(0, emptyDump.serializedHistograms.length);
        assertEquals(0, emptyDump.serializedMeters.length);
    }

    @Test
    public void testHandleOversizedMetricMessage() throws Exception {
        final long sizeLimit = 200L;
        final MetricQueryService queryService = createAndStartMetricQueryService(sizeLimit);

        final TaskManagerMetricGroup tm =
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

        final String gaugeValue = "Hello";
        final long requiredGaugesToExceedLimit = sizeLimit / gaugeValue.length() + 1;
        List<Tuple2<String, Gauge<String>>> gauges =
                LongStream.range(0, requiredGaugesToExceedLimit)
                        .mapToObj(x -> Tuple2.of("gauge" + x, (Gauge<String>) () -> "Hello" + x))
                        .collect(Collectors.toList());
        gauges.forEach(gauge -> queryService.addMetric(gauge.f0, gauge.f1, tm));

        queryService.addMetric("counter", new SimpleCounter(), tm);
        queryService.addMetric("histogram", new TestHistogram(), tm);
        queryService.addMetric("meter", new TestMeter(), tm);

        MetricDumpSerialization.MetricSerializationResult dump =
                queryService.queryMetrics(TIMEOUT).get();

        assertTrue(dump.serializedCounters.length > 0);
        assertEquals(1, dump.numCounters);
        assertTrue(dump.serializedMeters.length > 0);
        assertEquals(1, dump.numMeters);

        // gauges exceeded the size limit and will be excluded
        assertEquals(0, dump.serializedGauges.length);
        assertEquals(0, dump.numGauges);

        assertTrue(dump.serializedHistograms.length > 0);
        assertEquals(1, dump.numHistograms);

        // unregister all but one gauge to ensure gauges are reported again if the remaining fit
        for (int x = 1; x < gauges.size(); x++) {
            queryService.removeMetric(gauges.get(x).f1);
        }

        MetricDumpSerialization.MetricSerializationResult recoveredDump =
                queryService.queryMetrics(TIMEOUT).get();

        assertTrue(recoveredDump.serializedCounters.length > 0);
        assertEquals(1, recoveredDump.numCounters);
        assertTrue(recoveredDump.serializedMeters.length > 0);
        assertEquals(1, recoveredDump.numMeters);
        assertTrue(recoveredDump.serializedGauges.length > 0);
        assertEquals(1, recoveredDump.numGauges);
        assertTrue(recoveredDump.serializedHistograms.length > 0);
        assertEquals(1, recoveredDump.numHistograms);
    }

    @Test
    public void queryJobMetrics() {
        final MetricQueryService queryService = createAndStartMetricQueryService(Long.MAX_VALUE);

        final TaskManagerMetricGroup taskManagerMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup();

        final List<String> jobIds = new ArrayList<>();
        final int numJobs = 10;
        final int numTasks = 5;

        for (int jobIdx = 0; jobIdx < numJobs; jobIdx++) {
            final JobID jobId = new JobID();
            final TaskManagerJobMetricGroup jobMetricGroup =
                    taskManagerMetricGroup.addJob(jobId, "test-job-" + jobIdx);
            for (int taskIdx = 0; taskIdx < numTasks; taskIdx++) {
                final TaskMetricGroup taskMetricGroup =
                        jobMetricGroup.addTask(ExecutionAttemptID.randomId(), "task-" + taskIdx);

                final SimpleCounter firstCounter = new SimpleCounter();
                queryService.addMetric("firstCounter", firstCounter, taskMetricGroup);
                firstCounter.inc(jobIdx + 1);

                // this counter should be filtered out
                final SimpleCounter secondCounter = new SimpleCounter();
                queryService.addMetric("secondCounter", secondCounter, taskMetricGroup);
                secondCounter.inc(jobIdx + 1);
            }

            jobIds.add(jobId.toHexString());
        }

        final MetricDumpSerialization.MetricDumpDeserializer deserializer =
                new MetricDumpSerialization.MetricDumpDeserializer();

        for (String jobId : jobIds) {
            final MetricDumpSerialization.MetricSerializationResult serializedMetrics =
                    queryService
                            .queryJobMetrics(
                                    TIMEOUT,
                                    JobMetricsFilter.newBuilder(jobId)
                                            .withTaskMetrics("firstCounter")
                                            .build())
                            .join();
            final List<MetricDump> deserializedMetrics =
                    deserializer.deserialize(serializedMetrics);
            Assertions.assertThat(deserializedMetrics).hasSize(numTasks);
            for (MetricDump dump : deserializedMetrics) {
                Assertions.assertThat(dump.name).isEqualTo("firstCounter");
                Assertions.assertThat(dump.scopeInfo)
                        .isInstanceOf(QueryScopeInfo.TaskQueryScopeInfo.class);
            }
        }
    }

    private MetricQueryService createAndStartMetricQueryService(long sizeLimit) {
        final MetricQueryService queryService =
                MetricQueryService.createMetricQueryService(
                        rpcServiceExtension.getCustomExtension().getTestingRpcService(),
                        ResourceID.generate(),
                        sizeLimit);
        queryService.start();
        return queryService;
    }
}
