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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.datagen.source.TestDataGenerators;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.file.table.stream.compact.CompactOperator.COMPACTED_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/** Streaming sink File Compaction ITCase base, test checkpoint. */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
public abstract class CompactionITCaseBase extends StreamingTestBase {

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    private String resultPath;

    private List<Row> expectedRows;

    @BeforeEach
    protected void init() throws IOException {
        resultPath = TempDirUtils.newFolder(tempFolder()).toURI().toString();

        env().setParallelism(3);
        env().enableCheckpointing(100);

        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(Row.of(i, String.valueOf(i % 10), String.valueOf(i % 10)));
        }

        this.expectedRows = new ArrayList<>();
        this.expectedRows.addAll(rows);
        this.expectedRows.addAll(rows);
        this.expectedRows.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));

        RowTypeInfo rowTypeInfo =
                new RowTypeInfo(
                        new TypeInformation[] {Types.INT, Types.STRING, Types.STRING},
                        new String[] {"a", "b", "c"});

        DataStream<Row> stream =
                env().fromSource(
                                TestDataGenerators.fromDataWithSnapshotsLatch(rows, rowTypeInfo),
                                WatermarkStrategy.noWatermarks(),
                                "Test Source")
                        .filter((FilterFunction<Row>) value -> true)
                        .setParallelism(3); // to parallel tasks

        tEnv().createTemporaryView("my_table", stream);
    }

    protected abstract String partitionField();

    protected abstract void createTable(String path);

    protected abstract void createPartitionTable(String path);

    @TestTemplate
    void testSingleParallelism() throws Exception {
        innerTestNonPartition(1);
    }

    @TestTemplate
    void testNonPartition() throws Exception {
        innerTestNonPartition(3);
    }

    void innerTestNonPartition(int parallelism) throws Exception {
        createTable(resultPath);
        String sql =
                String.format(
                        "insert into sink_table /*+ OPTIONS('sink.parallelism' = '%d') */"
                                + " select * from my_table",
                        parallelism);
        tEnv().executeSql(sql).await();

        assertIterator(tEnv().executeSql("select * from sink_table").collect());

        assertFiles(new File(URI.create(resultPath)).listFiles(), false);
    }

    @TestTemplate
    void testPartition() throws Exception {
        createPartitionTable(resultPath);
        tEnv().executeSql("insert into sink_table select * from my_table").await();

        assertIterator(tEnv().executeSql("select * from sink_table").collect());

        File path = new File(URI.create(resultPath));
        assertThat(path.listFiles()).hasSize(10);

        for (int i = 0; i < 10; i++) {
            File partition = new File(path, partitionField() + "=" + i);
            assertFiles(partition.listFiles(), true);
        }
    }

    private void assertIterator(CloseableIterator<Row> iterator) throws Exception {
        List<Row> result = CollectionUtil.iteratorToList(iterator);
        iterator.close();
        result.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));
        assertThat(result).isEqualTo(expectedRows);
    }

    private void assertFiles(File[] files, boolean containSuccess) {
        File successFile = null;
        for (File file : files) {
            // exclude crc files
            if (file.isHidden()) {
                continue;
            }
            if (containSuccess && file.getName().equals("_SUCCESS")) {
                successFile = file;
            } else {
                assertThat(file.getName()).as(file.getName()).startsWith(COMPACTED_PREFIX);
            }
        }
        if (containSuccess) {
            assertThat(successFile).as("Should contains success file").isNotNull();
        }
    }
}
