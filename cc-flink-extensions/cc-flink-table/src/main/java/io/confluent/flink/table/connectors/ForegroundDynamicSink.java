/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

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
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;

import java.time.Duration;
import java.util.Objects;

/** Confluent result serving table sink for powering foreground queries. */
public class ForegroundDynamicSink implements DynamicTableSink {

    public static final String TRANSFORMATION_NAME = "cc-foreground-sink";

    public static final String ACCUMULATOR_NAME = "cc-foreground-sink";

    public static final String SINK_NAME = "cc-foreground-sink";

    private final MemorySize maxBatchSize;

    private final Duration socketTimeout;

    private final DataType consumedDataType;

    public ForegroundDynamicSink(
            MemorySize maxBatchSize, Duration socketTimeout, DataType consumedDataType) {
        this.maxBatchSize = maxBatchSize;
        this.socketTimeout = socketTimeout;
        this.consumedDataType = consumedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Should never be upsert as no primary key is set.
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> inputStream) {

                final TypeSerializer<RowData> serializer =
                        InternalSerializers.create(consumedDataType.getLogicalType());

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
        return new ForegroundDynamicSink(maxBatchSize, socketTimeout, consumedDataType);
    }

    @Override
    public String asSummaryString() {
        return ForegroundDynamicSink.class.getSimpleName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForegroundDynamicSink that = (ForegroundDynamicSink) o;
        return maxBatchSize.equals(that.maxBatchSize)
                && socketTimeout.equals(that.socketTimeout)
                && consumedDataType.equals(that.consumedDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxBatchSize, socketTimeout, consumedDataType);
    }
}
