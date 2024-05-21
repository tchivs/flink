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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.PerKeyStateDataViewStore;
import org.apache.flink.table.runtime.generated.AggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Process Function for RANGE clause event-time bounded OVER window.
 *
 * <p>E.g.: SELECT rowtime, b, c, min(c) OVER (PARTITION BY b ORDER BY rowtime RANGE BETWEEN
 * INTERVAL '4' SECOND PRECEDING AND CURRENT ROW), max(c) OVER (PARTITION BY b ORDER BY rowtime
 * RANGE BETWEEN INTERVAL '4' SECOND PRECEDING AND CURRENT ROW) FROM T.
 */
public class RowTimeRangeBoundedPrecedingFunction<K>
        extends KeyedProcessFunction<K, RowData, RowData> {
    private static final long serialVersionUID = 1L;
    private static final long stateVersionID = 1L;

    private static final Logger LOG =
            LoggerFactory.getLogger(RowTimeRangeBoundedPrecedingFunction.class);

    private final GeneratedAggsHandleFunction genAggsHandler;
    private final LogicalType[] accTypes;
    private final LogicalType[] inputFieldTypes;
    private final long precedingOffset;
    private final int rowTimeIdx;

    private transient JoinedRowData output;

    // the state which keeps the last triggering timestamp
    private transient ValueState<Long> lastTriggeringTsState;

    // the version to which the state is fully migrated
    private transient ValueState<Long> stateVersionState;

    // the state which used to materialize the accumulator for incremental calculation
    private transient ValueState<RowData> accState;

    // Stores pending records keyed by event time.
    // Records are added by processElement() and moved by onTimer() to accumulatedRecords.
    // The size is bounded by watermark delay, so access is relatively fast.
    // MapState is used for 3 reasons:
    // 1. We have to use timers to maintain time order in output; for that, we need lookup by time
    // 2. MapState iterator doesn't load everything into memory
    // 3. MapState iterator supports removals
    private transient MapState<Long, List<RowData>> pendingRecords;

    // Stores records that were added to function and need to be retracted at some point.
    // The size is bounded by precedingOffset.
    private transient MapState<Long, List<RowData>> accumulatedRecords;

    private transient AggsHandleFunction function;

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
    private transient Counter numLateRecordsDropped;

    @VisibleForTesting
    protected Counter getCounter() {
        return numLateRecordsDropped;
    }

    public RowTimeRangeBoundedPrecedingFunction(
            GeneratedAggsHandleFunction genAggsHandler,
            LogicalType[] accTypes,
            LogicalType[] inputFieldTypes,
            long precedingOffset,
            int rowTimeIdx) {
        this.genAggsHandler = genAggsHandler;
        this.accTypes = accTypes;
        this.inputFieldTypes = inputFieldTypes;
        this.precedingOffset = precedingOffset;
        this.rowTimeIdx = rowTimeIdx;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        function = genAggsHandler.newInstance(getRuntimeContext().getUserCodeClassLoader());
        function.open(new PerKeyStateDataViewStore(getRuntimeContext()));

        output = new JoinedRowData();

        lastTriggeringTsState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("lastTriggeringTsState", Types.LONG));

        stateVersionState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("stateVersionState", Types.LONG));

        accState =
                getRuntimeContext()
                        .getState(
                                new ValueStateDescriptor<>(
                                        "accState", InternalTypeInfo.ofFields(accTypes)));

        // input element are all binary row as they are came from network
        pendingRecords = createMapState("inputState");
        accumulatedRecords = createMapState("accumulatedState");

        // metrics
        this.numLateRecordsDropped =
                getRuntimeContext().getMetricGroup().counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
    }

    private MapState<Long, List<RowData>> createMapState(String name) {
        ExecutionConfig executionConfig = getRuntimeContext().getExecutionConfig();
        TypeSerializer<RowData> rowDataTypeSerializer =
                InternalTypeInfo.ofFields(inputFieldTypes).createSerializer(executionConfig);
        return getRuntimeContext()
                .getMapState(
                        new MapStateDescriptor<>(
                                name,
                                Types.LONG.createSerializer(executionConfig),
                                new ListSerializer<>(rowDataTypeSerializer)));
    }

    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<K, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {
        long triggeringTs = input.getLong(rowTimeIdx);

        Long lastTriggeringTs = lastTriggeringTsState.value();
        if (lastTriggeringTs != null && lastTriggeringTs >= triggeringTs) {
            numLateRecordsDropped.inc();
            return;
        }
        List<RowData> list = pendingRecords.get(triggeringTs);
        if (list == null) {
            list = new ArrayList<>();
            ctx.timerService().registerEventTimeTimer(triggeringTs);
        }
        list.add(input);
        pendingRecords.put(triggeringTs, list);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            Collector<RowData> out)
            throws Exception {
        function.setAccumulators(restoreOrCreateAccumulators());

        migrateStateIfNeeded(timestamp, ctx);

        retractAndRemove(timestamp);

        final List<RowData> newRecords = pendingRecords.get(timestamp);
        if (newRecords == null) {
            storeAccumulators();
            return;
        }

        RowData accValue = accumulate(newRecords);
        output(accValue, out, newRecords);
        pendingRecords.remove(timestamp);
        rememberToRetract(newRecords, ctx, getRetractTimestamp(timestamp));
        storeAccumulators();
        lastTriggeringTsState.update(timestamp);
    }

    private void migrateStateIfNeeded(
            long timestamp, KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx)
            throws Exception {
        Long migratedToVersion = stateVersionState.value();
        if (migratedToVersion != null && migratedToVersion >= stateVersionID) {
            return;
        }
        Long lastTriggeringTs = lastTriggeringTsState.value();
        if (lastTriggeringTs == null) {
            lastTriggeringTs = -1L;
        }
        LOG.info(
                "Migrate state from version {} to {}, last trigger ts={}, current ts={}",
                migratedToVersion,
                stateVersionID,
                lastTriggeringTs,
                timestamp);
        Iterator<Map.Entry<Long, List<RowData>>> it = pendingRecords.iterator();
        while (it.hasNext()) {
            Map.Entry<Long, List<RowData>> e = it.next();
            Long recordsTs = e.getKey();
            if (recordsTs > lastTriggeringTs) {
                // not in the window yet regardless of version - no need to migrate
                continue;
            }
            long retractTs = getRetractTimestamp(recordsTs);
            if (retractTs > timestamp) {
                rememberToRetract(e.getValue(), ctx, retractTs);
            } else {
                for (RowData rowData : e.getValue()) {
                    checkNotInterrupted();
                    function.retract(rowData);
                }
            }
            it.remove();
        }
        stateVersionState.update(stateVersionID);
    }

    private void retractAndRemove(long timestamp) throws Exception {
        LOG.debug("Retract and remove records at timestamp (-offset): {}", timestamp);
        final List<RowData> staleRecords = accumulatedRecords.get(timestamp);
        if (staleRecords != null) {
            for (RowData rowData : staleRecords) {
                checkNotInterrupted();
                function.retract(rowData);
            }
            accumulatedRecords.remove(timestamp);
        }
    }

    private RowData accumulate(List<RowData> newRecords) throws Exception {
        LOG.debug("Accumulate {} records", newRecords.size());
        for (RowData rowData : newRecords) {
            function.accumulate(rowData);
        }
        storeAccumulators();
        return function.getValue();
    }

    private void output(RowData value, Collector<RowData> output, List<RowData> records)
            throws InterruptedException {
        LOG.debug("Output result for {} records", records.size());
        for (RowData rowData : records) {
            checkNotInterrupted();
            output.collect(this.output.replace(rowData, value));
        }
    }

    private void rememberToRetract(
            List<RowData> records,
            KeyedProcessFunction<K, RowData, RowData>.OnTimerContext ctx,
            long retractTimestamp)
            throws Exception {
        // expect to transfer records once per timestamp, so just put not merge
        accumulatedRecords.put(retractTimestamp, records);
        ctx.timerService().registerEventTimeTimer(retractTimestamp);
    }

    private long getRetractTimestamp(long atTimestamp) {
        return atTimestamp + precedingOffset + 1;
    }

    @Override
    public void close() throws Exception {
        if (null != function) {
            function.close();
        }
    }

    private static void checkNotInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }

    private void storeAccumulators() throws Exception {
        accState.update(function.getAccumulators());
    }

    private RowData restoreOrCreateAccumulators() throws Exception {
        RowData accumulators = accState.value();
        if (accumulators == null) {
            accumulators = function.createAccumulators();
        }
        return accumulators;
    }
}
