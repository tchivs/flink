/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.connectors.ConfluentManagedKafkaDeserializationSchema.MetadataConverter;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableUtils.DynamicTableParameters;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.confluent.flink.table.connectors.ConfluentManagedKafkaSerializationSchema.ROW_KIND_HEADER_KEY;

/** {@link DynamicTableSource} for Confluent-native tables. */
@Confluent
public class ConfluentManagedTableSource
        implements ScanTableSource,
                SupportsReadingMetadata,
                SupportsWatermarkPushDown,
                SupportsSourceWatermark {

    private static final String KAFKA_TRANSFORMATION = "kafka";

    private static final String VALUE_METADATA_PREFIX = "value.";

    private static final ChangelogMode KAFKA_UPSERT_CHANGELOG_MODE =
            ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.UPDATE_AFTER)
                    .addContainedKind(RowKind.DELETE)
                    .build();

    // --------------------------------------------------------------------------------------------
    // Immutable attributes
    // --------------------------------------------------------------------------------------------

    /** All configuration shared with the {@link ConfluentManagedTableSink}. */
    private final DynamicTableParameters parameters;

    /** Optional format for decoding keys from Kafka. */
    private final @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat;

    /** Format for decoding values from Kafka. */
    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------

    /** Data type that describes the final output of the source. */
    private DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    private List<String> metadataKeys;

    /** Watermark strategy that is used to generate per-partition watermark. */
    private @Nullable WatermarkStrategy<RowData> watermarkStrategy;

    /** Whether {@code SOURCE_WATERMARK} is declared and should be applied. */
    private boolean applySourceWatermark;

    public ConfluentManagedTableSource(
            DynamicTableParameters parameters,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat) {
        this.parameters = Preconditions.checkNotNull(parameters, "Parameters must not be null");
        this.keyDecodingFormat = keyDecodingFormat;
        this.valueDecodingFormat =
                Preconditions.checkNotNull(valueDecodingFormat, "Value format must not be null");
        this.producedDataType = parameters.physicalDataType;
        this.metadataKeys = Collections.emptyList();
    }

    public DynamicTableParameters getParameters() {
        return parameters;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        final ManagedChangelogMode tableMode = parameters.tableMode;
        final ChangelogMode formatMode = valueDecodingFormat.getChangelogMode();

        // Check whether the upsert should be managed by Kafka.
        if (tableMode == ManagedChangelogMode.UPSERT && formatMode.containsOnly(RowKind.INSERT)) {
            // We cannot trust Kafka to have a proper Flink changelog.
            // INSERT is omitted to force a changelog normalization in the planner.
            return KAFKA_UPSERT_CHANGELOG_MODE;
        }
        return parameters.tableMode.toChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final DeserializationSchema<RowData> keyDeserialization =
                createDeserialization(
                        context, keyDecodingFormat, parameters.keyProjection, parameters.keyPrefix);

        final DeserializationSchema<RowData> valueDeserialization =
                createDeserialization(
                        context, valueDecodingFormat, parameters.valueProjection, null);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(producedDataType);

        final KafkaSource<RowData> kafkaSource =
                createKafkaSource(keyDeserialization, valueDeserialization, producedTypeInfo);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                DataStreamSource<RowData> sourceStream =
                        execEnv.fromSource(
                                kafkaSource,
                                getWatermarkStrategy(),
                                "KafkaSource-" + parameters.tableIdentifier);
                providerContext.generateUid(KAFKA_TRANSFORMATION).ifPresent(sourceStream::uid);
                return sourceStream;
            }

            @Override
            public boolean isBounded() {
                return kafkaSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        final ConfluentManagedTableSource copy =
                new ConfluentManagedTableSource(parameters, keyDecodingFormat, valueDecodingFormat);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Confluent table source";
    }

    // --------------------------------------------------------------------------------------------
    // SupportsReadingMetadata
    // --------------------------------------------------------------------------------------------

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();

        // according to convention, the order of the final row must be
        // PHYSICAL + FORMAT METADATA + CONNECTOR METADATA
        // where the format metadata has highest precedence

        // add value format metadata with prefix
        valueDecodingFormat
                .listReadableMetadata()
                .forEach((key, value) -> metadataMap.put(VALUE_METADATA_PREFIX + key, value));

        // add connector metadata
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.putIfAbsent(m.key, m.dataType));

        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        // separate connector and format metadata
        final List<String> formatMetadataKeys =
                metadataKeys.stream()
                        .filter(k -> k.startsWith(VALUE_METADATA_PREFIX))
                        .collect(Collectors.toList());
        final List<String> connectorMetadataKeys = new ArrayList<>(metadataKeys);
        connectorMetadataKeys.removeAll(formatMetadataKeys);

        // push down format metadata
        final Map<String, DataType> formatMetadata = valueDecodingFormat.listReadableMetadata();
        if (formatMetadata.size() > 0) {
            final List<String> requestedFormatMetadataKeys =
                    formatMetadataKeys.stream()
                            .map(k -> k.substring(VALUE_METADATA_PREFIX.length()))
                            .collect(Collectors.toList());
            valueDecodingFormat.applyReadableMetadata(requestedFormatMetadataKeys);
        }

        this.metadataKeys = connectorMetadataKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public boolean supportsMetadataProjection() {
        return false;
    }

    // --------------------------------------------------------------------------------------------
    // SupportsWatermarkPushDown
    // --------------------------------------------------------------------------------------------

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    // --------------------------------------------------------------------------------------------
    // SupportsSourceWatermark
    // --------------------------------------------------------------------------------------------

    @Override
    public void applySourceWatermark() {
        this.applySourceWatermark = true;
    }

    // --------------------------------------------------------------------------------------------
    // Private helper methods
    // --------------------------------------------------------------------------------------------

    private KafkaSource<RowData> createKafkaSource(
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {

        final KafkaRecordDeserializationSchema<RowData> kafkaDeserializer =
                createKafkaDeserializationSchema(
                        keyDeserialization, valueDeserialization, producedTypeInfo);

        final KafkaSourceBuilder<RowData> kafkaSourceBuilder = KafkaSource.builder();

        kafkaSourceBuilder.setTopics(parameters.topic);

        switch (parameters.startupOptions.startupMode) {
            case EARLIEST_OFFSET:
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.earliest());
                break;
            case LATEST_OFFSET:
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                String offsetResetConfig =
                        parameters.properties.getProperty(
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                OffsetResetStrategy.NONE.name());
                OffsetResetStrategy offsetResetStrategy = getResetStrategy(offsetResetConfig);
                kafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.committedOffsets(offsetResetStrategy));
                break;
            case SPECIFIC_OFFSETS:
                Map<TopicPartition, Long> offsets = new HashMap<>();
                parameters.startupOptions.specificOffsets.forEach(
                        (tp, offset) ->
                                offsets.put(new TopicPartition(tp.topic, tp.partition), offset));
                kafkaSourceBuilder.setStartingOffsets(OffsetsInitializer.offsets(offsets));
                break;
            case TIMESTAMP:
                kafkaSourceBuilder.setStartingOffsets(
                        OffsetsInitializer.timestamp(
                                parameters.startupOptions.startupTimestampMillis));
                break;
        }

        switch (parameters.boundedOptions.boundedMode) {
            case UNBOUNDED:
                kafkaSourceBuilder.setUnbounded(new NoStoppingOffsetsInitializer());
                break;
            case LATEST_OFFSET:
                kafkaSourceBuilder.setBounded(OffsetsInitializer.latest());
                break;
            case GROUP_OFFSETS:
                kafkaSourceBuilder.setBounded(OffsetsInitializer.committedOffsets());
                break;
            case SPECIFIC_OFFSETS:
                Map<TopicPartition, Long> offsets = new HashMap<>();
                parameters.boundedOptions.specificOffsets.forEach(
                        (tp, offset) ->
                                offsets.put(new TopicPartition(tp.topic, tp.partition), offset));
                kafkaSourceBuilder.setBounded(OffsetsInitializer.offsets(offsets));
                break;
            case TIMESTAMP:
                kafkaSourceBuilder.setBounded(
                        OffsetsInitializer.timestamp(
                                parameters.boundedOptions.boundedTimestampMillis));
                break;
        }

        kafkaSourceBuilder.setProperties(parameters.properties).setDeserializer(kafkaDeserializer);

        return kafkaSourceBuilder.build();
    }

    private OffsetResetStrategy getResetStrategy(String offsetResetConfig) {
        return Arrays.stream(OffsetResetStrategy.values())
                .filter(ors -> ors.name().equals(offsetResetConfig.toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "%s can not be set to %s. Valid values: [%s]",
                                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                                offsetResetConfig,
                                                Arrays.stream(OffsetResetStrategy.values())
                                                        .map(Enum::name)
                                                        .map(String::toLowerCase)
                                                        .collect(Collectors.joining(",")))));
    }

    private KafkaRecordDeserializationSchema<RowData> createKafkaDeserializationSchema(
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            DeserializationSchema<RowData> valueDeserialization,
            TypeInformation<RowData> producedTypeInfo) {
        final MetadataConverter[] metadataConverters =
                metadataKeys.stream()
                        .map(
                                k ->
                                        Stream.of(ReadableMetadata.values())
                                                .filter(rm -> rm.key.equals(k))
                                                .findFirst()
                                                .orElseThrow(IllegalStateException::new))
                        .map(m -> m.converter)
                        .toArray(MetadataConverter[]::new);

        // check if connector metadata is used at all
        final boolean hasMetadata = metadataKeys.size() > 0;

        // adjust physical arity with value format's metadata
        final int adjustedPhysicalArity =
                DataType.getFieldDataTypes(producedDataType).size() - metadataKeys.size();

        // adjust value format projection to include value format's metadata columns at the end
        final int[] adjustedValueProjection =
                IntStream.concat(
                                IntStream.of(parameters.valueProjection),
                                IntStream.range(
                                        parameters.keyProjection.length
                                                + parameters.valueProjection.length,
                                        adjustedPhysicalArity))
                        .toArray();

        final ManagedChangelogMode deserializationMode;
        if (!valueDecodingFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            // format produces changelog
            deserializationMode = ManagedChangelogMode.APPEND;
        } else {
            // connector produces changelog
            deserializationMode = parameters.tableMode;
        }

        return new ConfluentManagedKafkaDeserializationSchema(
                adjustedPhysicalArity,
                keyDeserialization,
                parameters.keyProjection,
                valueDeserialization,
                adjustedValueProjection,
                hasMetadata,
                metadataConverters,
                producedTypeInfo,
                deserializationMode);
    }

    private @Nullable DeserializationSchema<RowData> createDeserialization(
            DynamicTableSource.Context context,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> format,
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
        return format.createRuntimeDecoder(context, physicalFormatDataType);
    }

    private WatermarkStrategy<RowData> getWatermarkStrategy() {
        if (watermarkStrategy != null) {
            return watermarkStrategy;
        } else if (!applySourceWatermark) {
            return WatermarkStrategy.noWatermarks();
        }

        if (parameters.watermarkOptions == null) {
            throw new IllegalArgumentException("Options for SOURCE_WATERMARK() are not provided.");
        }
        // Note: Currently, it is not possible to use SOURCE_WATERMARK() for any other column than
        // the built-in $rowtime system column coming directly from the Kafka message timestamp. If
        // we want to support other physical or metadata columns, we would need to pass information
        // about the position of the time attribute column. If we want to support other computed
        // columns, we would need a generated watermark strategy for evaluating the expressions.
        return VersionedWatermarkStrategy.forOptions(parameters.watermarkOptions);
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    enum ReadableMetadata {
        /**
         * Intended to be used as read-only system column that is provided by default.
         *
         * <p>Currently, this column is semantically equal to the 'timestamp' metadata key. But this
         * might change at some point.
         */
        SYSTEM_TIMESTAMP(
                "$rowtime",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record, boolean isRetract) {
                        return TimestampData.fromEpochMillis(record.timestamp());
                    }
                }),

        TOPIC(
                "topic",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record, boolean isRetract) {
                        return StringData.fromString(record.topic());
                    }
                }),

        PARTITION(
                "partition",
                DataTypes.INT().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record, boolean isRetract) {
                        return record.partition();
                    }
                }),

        HEADERS(
                "headers",
                // key and value of the map are nullable to make handling easier in queries
                DataTypes.MAP(DataTypes.STRING().nullable(), DataTypes.BYTES().nullable())
                        .notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record, boolean isRetract) {
                        final Map<StringData, byte[]> map = new HashMap<>();
                        for (Header header : record.headers()) {
                            final String key = header.key();
                            if (!isRetract || !key.equals(ROW_KIND_HEADER_KEY)) {
                                map.put(StringData.fromString(key), header.value());
                            }
                        }
                        return new GenericMapData(map);
                    }
                }),

        LEADER_EPOCH(
                "leader-epoch",
                DataTypes.INT().nullable(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record, boolean isRetract) {
                        return record.leaderEpoch().orElse(null);
                    }
                }),

        OFFSET(
                "offset",
                DataTypes.BIGINT().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record, boolean isRetract) {
                        return record.offset();
                    }
                }),

        TIMESTAMP(
                "timestamp",
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record, boolean isRetract) {
                        return TimestampData.fromEpochMillis(record.timestamp());
                    }
                }),

        TIMESTAMP_TYPE(
                "timestamp-type",
                DataTypes.STRING().notNull(),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object read(ConsumerRecord<?, ?> record, boolean isRetract) {
                        return StringData.fromString(record.timestampType().toString());
                    }
                });

        final String key;

        final DataType dataType;

        final MetadataConverter converter;

        ReadableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }
}
