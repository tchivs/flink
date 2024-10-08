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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** UT for BufferedKVExternalSorter. */
@ExtendWith(ParameterizedTestExtension.class)
class BufferedKVExternalSorterTest {
    private static final int PAGE_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

    private IOManager ioManager;
    private BinaryRowDataSerializer keySerializer;
    private BinaryRowDataSerializer valueSerializer;
    private NormalizedKeyComputer computer;
    private RecordComparator comparator;

    private int spillNumber;
    private int recordNumberPerFile;

    private Configuration conf;

    BufferedKVExternalSorterTest(int spillNumber, int recordNumberPerFile, boolean spillCompress) {
        ioManager = new IOManagerAsync();
        conf = new Configuration();
        conf.set(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES, 5);
        if (!spillCompress) {
            conf.set(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED, false);
        }
        this.spillNumber = spillNumber;
        this.recordNumberPerFile = recordNumberPerFile;
    }

    @Parameters(name = "spillNumber-{0} recordNumberPerFile-{1} spillCompress-{2}")
    private static List<Object[]> getDataSize() {
        List<Object[]> paras = new ArrayList<>();
        paras.add(new Object[] {3, 1000, true});
        paras.add(new Object[] {3, 1000, false});
        paras.add(new Object[] {10, 1000, true});
        paras.add(new Object[] {10, 1000, false});
        paras.add(new Object[] {10, 10000, true});
        paras.add(new Object[] {10, 10000, false});
        return paras;
    }

    @BeforeEach
    void beforeTest() throws InstantiationException, IllegalAccessException {
        this.ioManager = new IOManagerAsync();

        this.keySerializer = new BinaryRowDataSerializer(2);
        this.valueSerializer = new BinaryRowDataSerializer(2);

        this.computer = IntNormalizedKeyComputer.INSTANCE;
        this.comparator = IntRecordComparator.INSTANCE;
    }

    @AfterEach
    void afterTest() throws Exception {
        this.ioManager.close();
    }

    @TestTemplate
    void test() throws Exception {
        BufferedKVExternalSorter sorter =
                new BufferedKVExternalSorter(
                        ioManager,
                        keySerializer,
                        valueSerializer,
                        computer,
                        comparator,
                        PAGE_SIZE,
                        conf.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                        conf.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                        (int)
                                conf.get(
                                                ExecutionConfigOptions
                                                        .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                        .getBytes());
        TestMemorySegmentPool pool = new TestMemorySegmentPool(PAGE_SIZE);
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < spillNumber; i++) {
            ArrayList<MemorySegment> segments = new ArrayList<>();
            SimpleCollectingOutputView out =
                    new SimpleCollectingOutputView(segments, pool, PAGE_SIZE);
            writeKVToBuffer(keySerializer, valueSerializer, out, expected, recordNumberPerFile);
            sorter.sortAndSpill(segments, recordNumberPerFile, pool);
        }
        Collections.sort(expected);
        MutableObjectIterator<Tuple2<BinaryRowData, BinaryRowData>> iterator =
                sorter.getKVIterator();
        Tuple2<BinaryRowData, BinaryRowData> kv =
                new Tuple2<>(keySerializer.createInstance(), valueSerializer.createInstance());
        int count = 0;
        while ((kv = iterator.next(kv)) != null) {
            assertThat(kv.f0.getInt(0)).isEqualTo((int) expected.get(count));
            assertThat(kv.f1.getInt(0)).isEqualTo(expected.get(count) * -3 + 177);
            count++;
        }
        assertThat(count).isEqualTo(expected.size());
        sorter.close();
    }

    private void writeKVToBuffer(
            BinaryRowDataSerializer keySerializer,
            BinaryRowDataSerializer valueSerializer,
            SimpleCollectingOutputView out,
            List<Integer> expecteds,
            int length)
            throws IOException {
        Random random = new Random();
        int stringLength = 30;
        for (int i = 0; i < length; i++) {
            BinaryRowData key = randomRow(random, stringLength);
            BinaryRowData val = key.copy();
            val.setInt(0, val.getInt(0) * -3 + 177);
            expecteds.add(key.getInt(0));
            keySerializer.serializeToPages(key, out);
            valueSerializer.serializeToPages(val, out);
        }
    }

    public static BinaryRowData randomRow(Random random, int stringLength) {
        BinaryRowData row = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, random.nextInt());
        writer.writeString(1, StringData.fromString(RandomStringUtils.random(stringLength)));
        writer.complete();
        return row;
    }
}
