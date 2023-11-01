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

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.AutopilotMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AutopilotMetricsRequestBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AutopilotMetricsResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.JobMetricsFilter;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;

import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** Handler for the Autopilot Metrics endpoint. */
@Confluent
public class AutopilotMetricsHandler
        extends AbstractRestHandler<
                RestfulGateway,
                AutopilotMetricsRequestBody,
                AutopilotMetricsResponseBody,
                JobMessageParameters> {

    private final MetricDumpSerialization.MetricDumpDeserializer deserializer =
            new MetricDumpSerialization.MetricDumpDeserializer();

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

    private final Executor executor;

    private final MetricQueryServiceRetriever queryServiceRetriever;

    public AutopilotMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> headers,
            GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            MetricQueryServiceRetriever queryServiceRetriever,
            Executor executor) {
        super(leaderRetriever, timeout, headers, AutopilotMetricsHeaders.getInstance());
        this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
        this.queryServiceRetriever = queryServiceRetriever;
        this.executor = executor;
    }

    @Override
    protected final CompletableFuture<AutopilotMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<AutopilotMetricsRequestBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        final JobMetricsFilter filter = request.getRequestBody().toJobMetricsFilter(jobId);
        return resourceManagerGatewayRetriever
                .getFuture()
                .thenCompose(gw -> gw.requestTaskManagerMetricQueryServiceAddresses(timeout, jobId))
                .thenApply(
                        addresses ->
                                addresses.stream()
                                        .map(t -> t.f1)
                                        .map(queryServiceRetriever::retrieveService)
                                        .map(
                                                f ->
                                                        f.thenComposeAsync(
                                                                gw -> queryMetrics(gw, filter),
                                                                executor))
                                        .collect(Collectors.toList()))
                .thenCompose(FutureUtils::combineAll)
                .thenApply(
                        responses -> {
                            // No need for synchronization, but we need to flush the cpu caches.
                            synchronized (new Object()) {
                                return AutopilotMetricsResponseBody.combine(responses);
                            }
                        });
    }

    private CompletableFuture<AutopilotMetricsResponseBody> queryMetrics(
            MetricQueryServiceGateway queryServiceGateway, JobMetricsFilter filter) {
        return queryServiceGateway
                .queryJobMetrics(timeout, filter)
                .thenApply(
                        result -> {
                            final AutopilotMetricsResponseBody response =
                                    new AutopilotMetricsResponseBody();
                            CompletionException exception = null;
                            for (MetricDump metricDump : deserializer.deserialize(result)) {
                                try {
                                    response.addMetric(metricDump, result.timestamp);
                                } catch (ParseException pe) {
                                    exception =
                                            ExceptionUtils.firstOrSuppressed(
                                                    new CompletionException(
                                                            String.format(
                                                                    "Unable to parse metric [%s].",
                                                                    metricDump.name),
                                                            pe),
                                                    exception);
                                }
                            }
                            if (exception != null) {
                                throw exception;
                            }
                            return response;
                        });
    }
}
