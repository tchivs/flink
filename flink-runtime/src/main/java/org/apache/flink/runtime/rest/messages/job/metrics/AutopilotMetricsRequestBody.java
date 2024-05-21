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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.webmonitor.retriever.JobMetricsFilter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

/** Response body of the Autopilot Metrics endpoint. */
@Confluent
public class AutopilotMetricsRequestBody implements RequestBody {

    private static final String FIELD_TASK_MANAGER_METRICS = "taskManagerMetrics";
    private static final String FIELD_TASK_METRICS = "taskMetrics";
    private static final String FIELD_OPERATOR_METRICS = "operatorMetrics";

    public JobMetricsFilter toJobMetricsFilter(JobID jobId) {
        return JobMetricsFilter.newBuilder(jobId.toHexString())
                .withTaskManagerMetrics(
                        taskManagerMetrics.toArray(new String[taskManagerMetrics.size()]))
                .withTaskMetrics(taskMetrics.toArray(new String[taskMetrics.size()]))
                .withOperatorMetrics(operatorMetrics.toArray(new String[operatorMetrics.size()]))
                .build();
    }

    @JsonProperty(FIELD_TASK_MANAGER_METRICS)
    private final Set<String> taskManagerMetrics;

    @JsonProperty(FIELD_TASK_METRICS)
    private final Set<String> taskMetrics;

    @JsonProperty(FIELD_OPERATOR_METRICS)
    private final Set<String> operatorMetrics;

    @JsonCreator
    public AutopilotMetricsRequestBody(
            @JsonProperty(FIELD_TASK_MANAGER_METRICS) Set<String> taskManagerMetrics,
            @JsonProperty(FIELD_TASK_METRICS) Set<String> taskMetrics,
            @JsonProperty(FIELD_OPERATOR_METRICS) Set<String> operatorMetrics) {
        this.taskManagerMetrics = taskManagerMetrics;
        this.taskMetrics = taskMetrics;
        this.operatorMetrics = operatorMetrics;
    }

    public Set<String> getTaskManagerMetrics() {
        return taskManagerMetrics;
    }

    public Set<String> getTaskMetrics() {
        return taskMetrics;
    }

    public Set<String> getOperatorMetrics() {
        return operatorMetrics;
    }
}
