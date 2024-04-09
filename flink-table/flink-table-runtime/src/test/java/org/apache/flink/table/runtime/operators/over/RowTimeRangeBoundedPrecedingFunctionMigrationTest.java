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

import org.apache.flink.FlinkVersion;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MigrationTest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static org.apache.flink.streaming.util.OperatorSnapshotUtil.getResourceFilename;
import static org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunctionTest.createHarness;
import static org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunctionTest.snapshot;
import static org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunctionTest.testCorrectnessAfterSnapshot;
import static org.apache.flink.table.runtime.operators.over.RowTimeRangeBoundedPrecedingFunctionTest.testCorrectnessBeforeSnapshot;

/** Test for {@link RowTimeRangeBoundedPrecedingFunction} migration. */
@RunWith(Parameterized.class)
public class RowTimeRangeBoundedPrecedingFunctionMigrationTest implements MigrationTest {

    private final FlinkVersion migrateVersion;

    public RowTimeRangeBoundedPrecedingFunctionMigrationTest(FlinkVersion migrateVersion) {
        this.migrateVersion = migrateVersion;
    }

    @Parameterized.Parameters(name = "Migration Savepoint: {0}")
    public static Collection<FlinkVersion> parameters() {
        return FlinkVersion.rangeOf(
                FlinkVersion.v1_18, MigrationTest.getMostRecentlyPublishedVersion());
    }

    @Test
    public void testMigration() throws Exception {
        String path = getResourceFilename(getSnapshotFilename(migrateVersion));
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness(path)) {
            testCorrectnessAfterSnapshot(harness);
        }
    }

    @SnapshotsGenerator
    public void writeSnapshot(FlinkVersion version) throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness(null)) {
            testCorrectnessBeforeSnapshot(harness);
            snapshot(harness, getSnapshotPath(version));
        }
    }

    private static String getSnapshotPath(FlinkVersion version) {
        return "src/test/resources/" + getSnapshotFilename(version);
    }

    private static String getSnapshotFilename(FlinkVersion version) {
        return "row-time-range-bounded-preceding-function-migration-flink-" + version + "-snapshot";
    }
}
