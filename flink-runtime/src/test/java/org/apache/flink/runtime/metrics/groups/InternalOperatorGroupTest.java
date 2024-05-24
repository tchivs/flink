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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.MetricRegistryTestUtils;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Tests for the {@link InternalOperatorMetricGroup}. */
public class InternalOperatorGroupTest extends TestLogger {

    private MetricRegistryImpl registry;

    @Before
    public void setup() {
        registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());
    }

    @After
    public void teardown() throws Exception {
        if (registry != null) {
            registry.closeAsync().get();
        }
    }

    @Test
    public void testGenerateScopeDefault() throws Exception {
        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup =
                tmGroup.addJob(new JobID(), "myJobName")
                        .addTask(createExecutionAttemptId(new JobVertexID(), 11, 0), "aTaskName");
        Map<String, String> additionalVariables = new HashMap<>();
        additionalVariables.put("foo", "42");
        additionalVariables.put("bar", "44");
        InternalOperatorMetricGroup opGroup =
                taskGroup.getOrAddOperator(new OperatorID(), "myOpName", additionalVariables);

        assertArrayEquals(
                new String[] {
                    "theHostName", "taskmanager", "test-tm-id", "myJobName", "myOpName", "11"
                },
                opGroup.getScopeComponents());

        assertEquals(
                "theHostName.taskmanager.test-tm-id.myJobName.myOpName.11.name",
                opGroup.getMetricIdentifier("name"));

        assertThat(opGroup.getAllVariables()).contains(entry("foo", "42"), entry("bar", "44"));
    }

    @Test
    public void testGenerateScopeCustom() throws Exception {
        Configuration cfg = new Configuration();
        cfg.setString(
                MetricOptions.SCOPE_NAMING_OPERATOR,
                "<tm_id>.<job_id>.<task_id>.<operator_name>.<operator_id>");
        MetricRegistryImpl registry =
                new MetricRegistryImpl(MetricRegistryTestUtils.fromConfiguration(cfg));
        try {
            String tmID = "test-tm-id";
            JobID jid = new JobID();
            JobVertexID vertexId = new JobVertexID();
            OperatorID operatorID = new OperatorID();
            String operatorName = "operatorName";

            InternalOperatorMetricGroup operatorGroup =
                    TaskManagerMetricGroup.createTaskManagerMetricGroup(
                                    registry, "theHostName", new ResourceID(tmID))
                            .addJob(jid, "myJobName")
                            .addTask(createExecutionAttemptId(vertexId, 13, 2), "aTaskname")
                            .getOrAddOperator(operatorID, operatorName, Collections.emptyMap());

            assertArrayEquals(
                    new String[] {
                        tmID,
                        jid.toString(),
                        vertexId.toString(),
                        operatorName,
                        operatorID.toString()
                    },
                    operatorGroup.getScopeComponents());

            assertEquals(
                    String.format(
                            "%s.%s.%s.%s.%s.name", tmID, jid, vertexId, operatorName, operatorID),
                    operatorGroup.getMetricIdentifier("name"));
        } finally {
            registry.closeAsync().get();
        }
    }

    @Test
    public void testIOMetricGroupInstantiation() throws Exception {
        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup =
                tmGroup.addJob(new JobID(), "myJobName")
                        .addTask(createExecutionAttemptId(new JobVertexID(), 11, 0), "aTaskName");
        InternalOperatorMetricGroup opGroup =
                taskGroup.getOrAddOperator(new OperatorID(), "myOpName", Collections.emptyMap());

        assertNotNull(opGroup.getIOMetricGroup());
        assertNotNull(opGroup.getIOMetricGroup().getNumRecordsInCounter());
        assertNotNull(opGroup.getIOMetricGroup().getNumRecordsOutCounter());
    }

    @Test
    public void testVariables() {
        JobID jid = new JobID();
        JobVertexID tid = new JobVertexID();
        ExecutionAttemptID eid = createExecutionAttemptId(tid, 11, 0);
        OperatorID oid = new OperatorID();

        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "theHostName", new ResourceID("test-tm-id"));

        TaskMetricGroup taskGroup = tmGroup.addJob(jid, "myJobName").addTask(eid, "aTaskName");
        InternalOperatorMetricGroup opGroup =
                taskGroup.getOrAddOperator(oid, "myOpName", Collections.emptyMap());

        Map<String, String> variables = opGroup.getAllVariables();

        testVariable(variables, ScopeFormat.SCOPE_HOST, "theHostName");
        testVariable(variables, ScopeFormat.SCOPE_TASKMANAGER_ID, "test-tm-id");
        testVariable(variables, ScopeFormat.SCOPE_JOB_ID, jid.toString());
        testVariable(variables, ScopeFormat.SCOPE_JOB_NAME, "myJobName");
        testVariable(variables, ScopeFormat.SCOPE_TASK_VERTEX_ID, tid.toString());
        testVariable(variables, ScopeFormat.SCOPE_TASK_NAME, "aTaskName");
        testVariable(variables, ScopeFormat.SCOPE_TASK_ATTEMPT_ID, eid.toString());
        testVariable(variables, ScopeFormat.SCOPE_TASK_SUBTASK_INDEX, "11");
        testVariable(variables, ScopeFormat.SCOPE_TASK_ATTEMPT_NUM, "0");
        testVariable(variables, ScopeFormat.SCOPE_OPERATOR_ID, oid.toString());
        testVariable(variables, ScopeFormat.SCOPE_OPERATOR_NAME, "myOpName");
    }

    private static void testVariable(
            Map<String, String> variables, String key, String expectedValue) {
        String actualValue = variables.get(key);
        assertNotNull(actualValue);
        assertEquals(expectedValue, actualValue);
    }

    @Test
    public void testCreateQueryServiceMetricInfo() {
        JobID jid = new JobID();
        JobVertexID vid = new JobVertexID();
        ExecutionAttemptID eid = createExecutionAttemptId(vid, 4, 5);
        OperatorID oid = new OperatorID();
        TaskManagerMetricGroup tm =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));

        TaskMetricGroup task = tm.addJob(jid, "jobname").addTask(eid, "taskName");
        InternalOperatorMetricGroup operator =
                task.getOrAddOperator(oid, "operator", Collections.emptyMap());

        QueryScopeInfo.OperatorQueryScopeInfo info =
                operator.createQueryServiceMetricInfo(new DummyCharacterFilter());
        assertEquals("", info.scope);
        assertEquals(jid.toString(), info.jobID);
        assertEquals(vid.toString(), info.vertexID);
        assertEquals(4, info.subtaskIndex);
        assertEquals("operator", info.operatorName);
    }
}
