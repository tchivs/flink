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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.DashboardConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler which returns the dashboard configuration. */
public class DashboardConfigHandler
        extends AbstractRestHandler<
                RestfulGateway, EmptyRequestBody, DashboardConfiguration, EmptyMessageParameters> {

    private final DashboardConfiguration dashboardConfiguration;

    public DashboardConfigHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, DashboardConfiguration, EmptyMessageParameters>
                    messageHeaders,
            long refreshInterval,
            boolean webSubmitEnabled,
            boolean webCancelEnabled,
            boolean webRescaleEnabled) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);

        dashboardConfiguration =
                DashboardConfiguration.from(
                        refreshInterval,
                        ZonedDateTime.now(),
                        webSubmitEnabled,
                        webCancelEnabled,
                        webRescaleEnabled,
                        false);
    }

    @Override
    public CompletableFuture<DashboardConfiguration> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway) {
        return CompletableFuture.completedFuture(dashboardConfiguration);
    }
}
