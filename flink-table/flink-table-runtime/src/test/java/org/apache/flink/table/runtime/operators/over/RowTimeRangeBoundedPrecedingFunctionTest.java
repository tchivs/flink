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

package org.apache.flink.table.runtime.operators.over;

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowTimeRangeBoundedPrecedingFunction}. */
public class RowTimeRangeBoundedPrecedingFunctionTest extends RowTimeOverWindowTestBase {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
    private static final int RESULT_POS = 3;

    @Test
    public void testCorrectness() throws Exception {
        String snapshotPath = TEMP_FOLDER.newFile().getAbsolutePath();
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness(null)) {
            testCorrectnessBeforeSnapshot(harness);
            snapshot(harness, snapshotPath);
            try (OneInputStreamOperatorTestHarness<RowData, RowData> restored =
                    createHarness(snapshotPath)) {
                testCorrectnessAfterSnapshot(restored);
            }
        }
    }

    static void testCorrectnessBeforeSnapshot(
            OneInputStreamOperatorTestHarness<RowData, RowData> harness) throws Exception {
        harness.processElement(insertRecord("key", 10L, 1L));
        harness.processElement(insertRecord("key", 20L, 2L));
        assertThat(getDataOutput(harness)).isEmpty(); // no watermark yet

        harness.processWatermark(new Watermark(1L));
        assertResultAtIndex(harness, 0, 10L); // 10 at 1L

        harness.processWatermark(new Watermark(2L));
        assertResultAtIndex(harness, 1, 30L); // 10+20

        harness.processElement(insertRecord("key", 40L, 3L));
    }

    static void testCorrectnessAfterSnapshot(
            OneInputStreamOperatorTestHarness<RowData, RowData> harness) throws Exception {
        assertThat(getDataOutput(harness)).isEmpty(); // no watermark yet

        harness.processWatermark(new Watermark(4L));
        assertResultAtIndex(harness, 0, 70L); // 10+20+30+40, all before snapshot

        harness.processElement(insertRecord("key", 50L, 200L));
        harness.processWatermark(new Watermark(200L));
        assertResultAtIndex(harness, 1, 50L); // all but 50 retracted
    }

    @Test
    public void testStateCleanup() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(null);

        AbstractKeyedStateBackend stateBackend =
                (AbstractKeyedStateBackend) testHarness.getOperator().getKeyedStateBackend();

        assertThat(stateBackend.numKeyValueStateEntries())
                .as("Initial state is not empty")
                .isEqualTo(0);

        // put some records
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));

        testHarness.processWatermark(new Watermark(1000L));
        // at this moment we expect the function to have some records in state

        testHarness.processWatermark(new Watermark(4000L));
        // at this moment the function should have cleaned up states

        assertThat(stateBackend.numKeyValueStateEntries())
                .as("State has not been cleaned up")
                .isEqualTo(3);
    }

    @Test
    public void testLateRecordMetrics() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createHarness(null);
        @SuppressWarnings({"rawtypes", "unchecked"})
        RowTimeRangeBoundedPrecedingFunction<RowData> function =
                (RowTimeRangeBoundedPrecedingFunction<RowData>)
                        ((KeyedProcessOperator) testHarness.getOperator()).getUserFunction();
        Counter counter = function.getCounter();

        // put some records
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 100L));
        testHarness.processElement(insertRecord("key", 1L, 500L));

        testHarness.processWatermark(new Watermark(500L));

        // late record
        testHarness.processElement(insertRecord("key", 1L, 400L));

        assertThat(counter.getCount()).isEqualTo(1L);
    }

    private static List<JoinedRowData> getDataOutput(
            OneInputStreamOperatorTestHarness<RowData, RowData> testHarness) {
        return testHarness.getOutput().stream()
                .filter(r -> r instanceof StreamRecord)
                .map(r -> (JoinedRowData) ((StreamRecord<?>) r).getValue())
                .collect(Collectors.toList());
    }

    static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(@Nullable String path)
            throws Exception {
        RowTimeRangeBoundedPrecedingFunction<RowData> function =
                new RowTimeRangeBoundedPrecedingFunction<>(
                        aggsHandleFunction, accTypes, inputFieldTypes, 10, 2);
        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(function);

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        if (path != null) {
            testHarness.initializeState(path);
        }
        testHarness.open();
        return testHarness;
    }

    private static void assertResultAtIndex(
            OneInputStreamOperatorTestHarness<RowData, RowData> testHarness,
            int index,
            long expected) {
        assertThat(getDataOutput(testHarness).get(index).getLong(RESULT_POS)).isEqualTo(expected);
    }

    static void snapshot(
            OneInputStreamOperatorTestHarness<RowData, RowData> harness, String snapshotPath)
            throws Exception {
        OperatorSnapshotUtil.writeStateHandle(harness.snapshot(1L, 1L), snapshotPath);
    }
}
