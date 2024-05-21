/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor.retriever;

import org.apache.flink.annotation.Confluent;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** Data structure for defining what metrics should be filtered out. */
@Confluent
public class JobMetricsFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    public static Builder newBuilder(String jobId) {
        return new Builder(jobId);
    }

    /** Builder for {@link JobMetricsFilter}. */
    public static class Builder {

        private final String jobId;
        private final Set<String> taskManagerMetrics = new HashSet<>();
        private final Set<String> taskMetrics = new HashSet<>();
        private final Set<String> operatorMetrics = new HashSet<>();

        public Builder(String jobId) {
            this.jobId = jobId;
        }

        public Builder withTaskManagerMetrics(String... metricNames) {
            return withMetrics(taskManagerMetrics, metricNames);
        }

        public Builder withTaskMetrics(String... metricNames) {
            return withMetrics(taskMetrics, metricNames);
        }

        public Builder withOperatorMetrics(String... metricNames) {
            return withMetrics(operatorMetrics, metricNames);
        }

        public JobMetricsFilter build() {
            return new JobMetricsFilter(
                    jobId,
                    Collections.unmodifiableSet(new HashSet<>(taskManagerMetrics)),
                    Collections.unmodifiableSet(new HashSet<>(taskMetrics)),
                    Collections.unmodifiableSet(new HashSet<>(operatorMetrics)));
        }

        private Builder withMetrics(Set<String> requestedMetrics, String... metricNames) {
            for (String metricName : metricNames) {
                requestedMetrics.add(metricName);
            }
            return this;
        }
    }

    private final String jobId;
    private final Set<String> taskManagerMetrics;
    private final Set<String> taskMetrics;
    private final Set<String> operatorMetrics;

    private JobMetricsFilter(
            String jobId,
            Set<String> taskManagerMetrics,
            Set<String> taskMetrics,
            Set<String> operatorMetrics) {
        this.jobId = jobId;
        this.taskManagerMetrics = taskManagerMetrics;
        this.taskMetrics = taskMetrics;
        this.operatorMetrics = operatorMetrics;
    }

    public String getJobId() {
        return jobId;
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
