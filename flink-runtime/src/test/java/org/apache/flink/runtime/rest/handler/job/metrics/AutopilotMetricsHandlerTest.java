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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.dump.TestingMetricQueryServiceGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.AutopilotMetricsRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AutopilotMetricsResponseBody;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FutureUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

/** Tests for {@link AutopilotMetricsHandler}. */
@ExtendWith(TestLoggerExtension.class)
class AutopilotMetricsHandlerTest {

    private static MetricQueryServiceRetriever createQueryServiceRetriever(
            Map<String, MetricQueryServiceGateway> gateways) {
        return address -> {
            @Nullable final MetricQueryServiceGateway gateway = gateways.get(address);
            if (gateway != null) {
                return CompletableFuture.completedFuture(gateway);
            }
            return FutureUtils.completedExceptionally(new Exception("Gateway doesn't exist."));
        };
    }

    @Test
    void test() throws Exception {
        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder().build();

        final TestingResourceManagerGateway resourceManagerGateway =
                new TestingResourceManagerGateway();
        resourceManagerGateway.setRequestTaskManagerMetricQueryServiceAddressesFunction(
                jobId ->
                        CompletableFuture.completedFuture(
                                Arrays.asList(
                                        Tuple2.of(ResourceID.generate(), "first"),
                                        Tuple2.of(ResourceID.generate(), "second"))));

        final String[] taskManagers =
                new String[] {"tm-" + UUID.randomUUID(), "tm-" + UUID.randomUUID()};

        final Map<String, MetricQueryServiceGateway> gateways = new HashMap<>();
        gateways.put(
                "first",
                TestingMetricQueryServiceGateway.newBuilder()
                        .setQueryJobMetricsFunction(
                                filter ->
                                        CompletableFuture.completedFuture(
                                                createMetrics(taskManagers)))
                        .build());
        gateways.put(
                "second",
                TestingMetricQueryServiceGateway.newBuilder()
                        .setQueryJobMetricsFunction(
                                filter ->
                                        CompletableFuture.completedFuture(
                                                createMetrics(taskManagers)))
                        .build());

        try (final AutopilotMetricsHandler handler =
                new AutopilotMetricsHandler(
                        GatewayRetriever.resolved(dispatcherGateway),
                        Time.hours(1),
                        Collections.emptyMap(),
                        GatewayRetriever.resolved(resourceManagerGateway),
                        createQueryServiceRetriever(gateways),
                        Executors.newSingleThreadExecutor())) {

            final Map<String, String> pathParameters = new HashMap<>();
            pathParameters.put(JobIDPathParameter.KEY, JobID.generate().toHexString());

            final AutopilotMetricsResponseBody response =
                    handler.handleRequest(
                                    HandlerRequest.resolveParametersAndCreate(
                                            new AutopilotMetricsRequestBody(
                                                    Collections.emptySet(),
                                                    Collections.emptySet(),
                                                    Collections.emptySet()),
                                            handler.getMessageHeaders()
                                                    .getUnresolvedMessageParameters(),
                                            pathParameters,
                                            Collections.emptyMap(),
                                            Collections.emptyList()),
                                    dispatcherGateway)
                            .get();

            Assertions.assertThat(response.getTaskManagerMetrics()).hasSize(taskManagers.length);
            Assertions.assertThat(response.getTaskMetrics()).isEmpty();
            Assertions.assertThat(response.getOperatorMetrics()).isEmpty();
        }
    }

    private MetricDumpSerialization.MetricSerializationResult createMetrics(
            String... taskManagers) {
        final MetricDumpSerialization.MetricDumpSerializer serializer =
                new MetricDumpSerialization.MetricDumpSerializer();

        final Map<Counter, Tuple2<QueryScopeInfo, String>> counters = new HashMap<>();
        final Map<Gauge<?>, Tuple2<QueryScopeInfo, String>> gauges = new HashMap<>();

        for (String taskManager : taskManagers) {
            final QueryScopeInfo.TaskManagerQueryScopeInfo taskManagerQueryScopeInfo =
                    new QueryScopeInfo.TaskManagerQueryScopeInfo(taskManager);
            counters.put(new SimpleCounter(), Tuple2.of(taskManagerQueryScopeInfo, "foo"));
            counters.put(new SimpleCounter(), Tuple2.of(taskManagerQueryScopeInfo, "bar"));
            gauges.put(() -> 10L, Tuple2.of(taskManagerQueryScopeInfo, "foo"));
            gauges.put(() -> 10d, Tuple2.of(taskManagerQueryScopeInfo, "bar"));
        }

        return serializer.serialize(
                counters, gauges, Collections.emptyMap(), Collections.emptyMap());
    }
}
