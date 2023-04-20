/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Objects;

/** Confluent result serving table sink for powering foreground queries. */
@Confluent
public class ForegroundResultTableSink implements DynamicTableSink {

    public static final String TRANSFORMATION_NAME = "cc-foreground-sink";
    public static final String ACCUMULATOR_NAME = "cc-foreground-sink";
    public static final String SINK_NAME = "cc-foreground-sink";

    private final MemorySize maxBatchSize;
    private final Duration socketTimeout;
    private final DataType consumedDataType;
    private final ZoneId zoneId;

    private ChangelogMode changelogMode = ChangelogMode.all();

    public ForegroundResultTableSink(
            MemorySize maxBatchSize,
            Duration socketTimeout,
            DataType consumedDataType,
            ZoneId zoneId) {
        this.maxBatchSize = maxBatchSize;
        this.socketTimeout = socketTimeout;
        this.consumedDataType = consumedDataType;
        this.zoneId = zoneId;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        changelogMode = requestedMode;
        // Should never be upsert as no primary key is set.
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> inputStream) {

                final TypeSerializer<RowData> serializer =
                        new ForegroundResultJsonSerializer(
                                consumedDataType.getLogicalType(),
                                zoneId,
                                changelogMode.containsOnly(RowKind.INSERT));

                final CollectSinkOperatorFactory<RowData> factory =
                        new CollectSinkOperatorFactory<>(
                                serializer, ACCUMULATOR_NAME, maxBatchSize, socketTimeout);

                final CollectStreamSink<RowData> sink =
                        new CollectStreamSink<>(inputStream, factory);

                providerContext.generateUid(TRANSFORMATION_NAME).ifPresent(sink::uid);

                return sink.name(SINK_NAME);
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        final ForegroundResultTableSink copy =
                new ForegroundResultTableSink(
                        maxBatchSize, socketTimeout, consumedDataType, zoneId);
        copy.changelogMode = changelogMode;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return ForegroundResultTableSink.class.getSimpleName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForegroundResultTableSink sink = (ForegroundResultTableSink) o;
        return maxBatchSize.equals(sink.maxBatchSize)
                && socketTimeout.equals(sink.socketTimeout)
                && consumedDataType.equals(sink.consumedDataType)
                && zoneId.equals(sink.zoneId)
                && changelogMode.equals(sink.changelogMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxBatchSize, socketTimeout, consumedDataType, zoneId, changelogMode);
    }
}
