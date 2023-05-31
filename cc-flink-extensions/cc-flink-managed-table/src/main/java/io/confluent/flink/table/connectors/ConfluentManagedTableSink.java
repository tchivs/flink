/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.connectors.ConfluentManagedTableFactory.DynamicTableParameters;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** {@link DynamicTableSink} for Confluent-native tables. */
@Confluent
public class ConfluentManagedTableSink
        implements DynamicTableSink, SupportsWritingMetadata, SupportsPartitioning {

    // --------------------------------------------------------------------------------------------
    // Immutable attributes
    // --------------------------------------------------------------------------------------------

    /** All configuration shared with the {@link ConfluentManagedTableSource}. */
    private final DynamicTableParameters parameters;

    /** Optional format for encoding keys to Kafka. */
    private final @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;

    /** Format for encoding values to Kafka. */
    private final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type of consumed data type. */
    private DataType consumedDataType;

    /** Metadata that is appended at the end of a physical sink row. */
    private List<String> metadataKeys;

    public ConfluentManagedTableSink(
            DynamicTableParameters parameters,
            @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat) {
        this.parameters = Preconditions.checkNotNull(parameters, "Parameters must not be null");
        this.keyEncodingFormat = keyEncodingFormat;
        this.valueEncodingFormat =
                Preconditions.checkNotNull(valueEncodingFormat, "Value format must not be null");
        this.consumedDataType = parameters.physicalDataType;
        this.metadataKeys = Collections.emptyList();
    }

    public DynamicTableParameters getParameters() {
        return parameters;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Compared to the source implementation, we support INSERT in upsert mode.
        return parameters.tableMode.toChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final KafkaSinkBuilder<RowData> sinkBuilder = KafkaSink.builder();
        if (parameters.transactionalIdPrefix != null) {
            sinkBuilder.setTransactionalIdPrefix(parameters.transactionalIdPrefix);
            sinkBuilder.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE);
        } else {
            sinkBuilder.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
        }
        final KafkaSink<RowData> kafkaSink =
                sinkBuilder
                        .setKafkaProducerConfig(parameters.properties)
                        .setRecordSerializer(createKafkaSerializationSchema(context))
                        .build();

        return SinkV2Provider.of(kafkaSink);
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(WritableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
        this.consumedDataType = consumedDataType;
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        // Currently, this information is unused, but
        // we need to implement the interface to enable PARTITIONED BY syntax
    }

    @Override
    public DynamicTableSink copy() {
        final ConfluentManagedTableSink copy =
                new ConfluentManagedTableSink(parameters, keyEncodingFormat, valueEncodingFormat);
        copy.consumedDataType = consumedDataType;
        copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Confluent table sink";
    }

    // --------------------------------------------------------------------------------------------

    private KafkaRecordSerializationSchema<RowData> createKafkaSerializationSchema(
            Context context) {
        final SerializationSchema<RowData> keySerialization =
                createSerialization(
                        context, keyEncodingFormat, parameters.keyProjection, parameters.keyPrefix);

        final SerializationSchema<RowData> valueSerialization =
                createSerialization(context, valueEncodingFormat, parameters.valueProjection, null);

        final ChangelogMode formatMode = valueEncodingFormat.getChangelogMode();

        final ManagedChangelogMode serializationMode;
        final FlinkKafkaPartitioner<RowData> partitioner;
        if (!formatMode.containsOnly(RowKind.INSERT)) {
            // format produces changelog
            serializationMode = ManagedChangelogMode.APPEND;
            // default Kafka partitioner takes care (either round-robin or hash of key)
            partitioner = null;
        } else {
            // connector produces changelog
            serializationMode = parameters.tableMode;
            if (parameters.tableMode == ManagedChangelogMode.RETRACT && keyEncodingFormat == null) {
                partitioner = ConfluentManagedRetractPartitioner.INSTANCE;
            } else {
                // default Kafka partitioner takes care (either round-robin or hash of key)
                partitioner = null;
            }
        }

        final List<LogicalType> physicalChildren =
                parameters.physicalDataType.getLogicalType().getChildren();

        return new ConfluentManagedKafkaSerializationSchema(
                parameters.topic,
                partitioner,
                keySerialization,
                valueSerialization,
                getFieldGetters(physicalChildren, parameters.keyProjection),
                getFieldGetters(physicalChildren, parameters.valueProjection),
                hasMetadata(),
                getMetadataPositions(physicalChildren),
                serializationMode);
    }

    private int[] getMetadataPositions(List<LogicalType> physicalChildren) {
        return Stream.of(WritableMetadata.values())
                .mapToInt(
                        m -> {
                            final int pos = metadataKeys.indexOf(m.key);
                            if (pos < 0) {
                                return -1;
                            }
                            return physicalChildren.size() + pos;
                        })
                .toArray();
    }

    private boolean hasMetadata() {
        return !metadataKeys.isEmpty();
    }

    private RowData.FieldGetter[] getFieldGetters(
            List<LogicalType> physicalChildren, int[] keyProjection) {
        return Arrays.stream(keyProjection)
                .mapToObj(
                        targetField ->
                                RowData.createFieldGetter(
                                        physicalChildren.get(targetField), targetField))
                .toArray(RowData.FieldGetter[]::new);
    }

    private @Nullable SerializationSchema<RowData> createSerialization(
            DynamicTableSink.Context context,
            @Nullable EncodingFormat<SerializationSchema<RowData>> format,
            int[] projection,
            @Nullable String prefix) {
        if (format == null) {
            return null;
        }
        DataType physicalFormatDataType =
                Projection.of(projection).project(parameters.physicalDataType);
        if (prefix != null) {
            physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
        }
        return format.createRuntimeEncoder(context, physicalFormatDataType);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum WritableMetadata {
        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        final MapData map = row.getMap(pos);
                        final ArrayData keyArray = map.keyArray();
                        final ArrayData valueArray = map.valueArray();
                        // +1 for adding a retraction flag
                        final List<Header> headers = new ArrayList<>(keyArray.size() + 1);
                        for (int i = 0; i < keyArray.size(); i++) {
                            if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                                final String key = keyArray.getString(i).toString();
                                final byte[] value = valueArray.getBinary(i);
                                headers.add(new KafkaHeader(key, value));
                            }
                        }
                        return headers;
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(RowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getTimestamp(pos, 3).getMillisecond();
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    interface MetadataConverter extends Serializable {
        Object read(RowData consumedRow, int pos);
    }

    // --------------------------------------------------------------------------------------------

    static class KafkaHeader implements Header {

        private final String key;

        private final byte[] value;

        KafkaHeader(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public byte[] value() {
            return value;
        }
    }
}
