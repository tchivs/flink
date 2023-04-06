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

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.job.metrics.JobMetricsHandler;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** {@link MessageHeaders} for {@link JobMetricsHandler}. */
@Confluent
public final class AutopilotMetricsHeaders
        implements RuntimeMessageHeaders<
                AutopilotMetricsRequestBody, AutopilotMetricsResponseBody, JobMessageParameters> {

    private static final AutopilotMetricsHeaders INSTANCE = new AutopilotMetricsHeaders();

    private AutopilotMetricsHeaders() {}

    @Override
    public JobMessageParameters getUnresolvedMessageParameters() {
        return new JobMessageParameters();
    }

    @Override
    public String getTargetRestEndpointURL() {
        return "/jobs/:" + JobIDPathParameter.KEY + "/autopilot-metrics";
    }

    public static AutopilotMetricsHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Provides access to job metrics.";
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public Class<AutopilotMetricsResponseBody> getResponseClass() {
        return AutopilotMetricsResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public Class<AutopilotMetricsRequestBody> getRequestClass() {
        return AutopilotMetricsRequestBody.class;
    }

    @Override
    public String operationId() {
        return "getAutopilotMetrics";
    }
}
