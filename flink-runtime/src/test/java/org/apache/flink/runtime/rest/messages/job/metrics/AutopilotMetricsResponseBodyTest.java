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

import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Tests for the {@link AutopilotMetricsResponseBody}. */
public class AutopilotMetricsResponseBodyTest
        extends RestResponseMarshallingTestBase<AutopilotMetricsResponseBody> {

    @Override
    protected Class<AutopilotMetricsResponseBody> getTestResponseClass() {
        return AutopilotMetricsResponseBody.class;
    }

    @Override
    protected AutopilotMetricsResponseBody getTestResponseInstance() throws Exception {
        final Map<String, Map<String, Number>> taskManagerMetrics = new HashMap<>();
        final Map<String, Number> taskManagerCounters = new HashMap<>();
        taskManagerCounters.put("foo", 1);
        taskManagerCounters.put("bar", 2);
        taskManagerMetrics.putIfAbsent("counters", taskManagerCounters);
        return new AutopilotMetricsResponseBody(
                taskManagerMetrics, Collections.emptyMap(), Collections.emptyMap());
    }
}
