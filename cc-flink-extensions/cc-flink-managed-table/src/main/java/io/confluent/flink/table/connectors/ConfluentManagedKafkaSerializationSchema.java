/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableSink.KafkaHeader;
import io.confluent.flink.table.connectors.ConfluentManagedTableSink.WritableMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A specific {@link KafkaRecordSerializationSchema} for {@link ConfluentManagedTableSink}. It is
 * responsible for projection of keys and metadata columns and most importantly serializing changes
 * from the changelog.
 *
 * <p>It takes a {@link ManagedChangelogMode} parameter. The parameter is not the table mode but the
 * mode of the serialization schema. It decides about how to deal write {@link RowData} out to Kafka
 * records:
 *
 * <ul>
 *   <li>{@link ManagedChangelogMode#APPEND} means that the value is treated "as is". This means
 *       that the encoding format is responsible for the change flag. And could potentially "append"
 *       any kind of change.
 *   <li>{@link ManagedChangelogMode#RETRACT} uses a byte flag in the header for all kinds of
 *       changes. Insertion if no header is present.
 *   <li>{@link ManagedChangelogMode#UPSERT} uses null in the value for deletions. Otherwise update
 *       after.
 * </ul>
 */
@Confluent
public class ConfluentManagedKafkaSerializationSchema
        implements KafkaRecordSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    public static final String ROW_KIND_HEADER_KEY = "op";

    private final String topic;

    private final @Nullable SerializationSchema<RowData> keySerialization;

    private final SerializationSchema<RowData> valueSerialization;

    private final RowData.FieldGetter[] keyFieldGetters;

    private final RowData.FieldGetter[] valueFieldGetters;

    private final boolean hasMetadata;

    private final int[] metadataPositions;

    private final ManagedChangelogMode changelogMode;

    private final FlinkKafkaPartitioner<RowData> partitioner;

    ConfluentManagedKafkaSerializationSchema(
            String topic,
            @Nullable FlinkKafkaPartitioner<RowData> partitioner,
            @Nullable SerializationSchema<RowData> keySerialization,
            SerializationSchema<RowData> valueSerialization,
            RowData.FieldGetter[] keyFieldGetters,
            RowData.FieldGetter[] valueFieldGetters,
            boolean hasMetadata,
            int[] metadataPositions,
            ManagedChangelogMode changelogMode) {
        if (changelogMode == ManagedChangelogMode.UPSERT) {
            Preconditions.checkArgument(
                    keySerialization != null && keyFieldGetters.length > 0,
                    "Key must be set in upsert mode for serialization schema.");
        }
        this.topic = checkNotNull(topic);
        this.partitioner = partitioner;
        this.keySerialization = keySerialization;
        this.valueSerialization = checkNotNull(valueSerialization);
        this.keyFieldGetters = keyFieldGetters;
        this.valueFieldGetters = valueFieldGetters;
        this.hasMetadata = hasMetadata;
        this.metadataPositions = metadataPositions;
        this.changelogMode = changelogMode;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        if (keySerialization != null) {
            keySerialization.open(context);
        }
        if (partitioner != null) {
            partitioner.open(
                    sinkContext.getParallelInstanceId(),
                    sinkContext.getNumberOfParallelInstances());
        }
        valueSerialization.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RowData consumedRow, KafkaSinkContext context, Long timestamp) {
        final RowKind kind = consumedRow.getRowKind();

        // shortcut in case no input projection is required
        if (kind == RowKind.INSERT && keySerialization == null && !hasMetadata) {
            final byte[] valueSerialized = valueSerialization.serialize(consumedRow);
            return new ProducerRecord<>(
                    topic,
                    extractPartition(
                            consumedRow,
                            null,
                            valueSerialized,
                            context.getPartitionsForTopic(topic)),
                    null,
                    valueSerialized);
        }

        List<Header> headers = readMetadata(consumedRow, WritableMetadata.HEADERS);

        final byte[] keySerialized;
        if (keySerialization == null) {
            keySerialized = null;
        } else {
            final RowData keyRow = createProjectedRow(consumedRow, RowKind.INSERT, keyFieldGetters);
            keySerialized = keySerialization.serialize(keyRow);
        }

        final byte[] valueSerialized;
        if (changelogMode == ManagedChangelogMode.UPSERT) {
            if (kind == RowKind.DELETE || kind == RowKind.UPDATE_BEFORE) {
                // transform the message as the tombstone message
                valueSerialized = null;
            } else {
                // make the message to be INSERT to be compliant with the INSERT-only format
                final RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
                valueRow.setRowKind(RowKind.INSERT);
                valueSerialized = valueSerialization.serialize(valueRow);
            }
        } else if (changelogMode == ManagedChangelogMode.RETRACT) {
            // omit header if the change would be INSERT
            if (kind != RowKind.INSERT) {
                if (headers == null) {
                    headers = createRowKindOnlyHeaders(kind);
                } else {
                    headers.add(mapRowKindHeader(kind));
                }
            }

            final RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
            valueRow.setRowKind(RowKind.INSERT);
            valueSerialized = valueSerialization.serialize(valueRow);
        } else {
            final RowData valueRow = createProjectedRow(consumedRow, kind, valueFieldGetters);
            valueSerialized = valueSerialization.serialize(valueRow);
        }

        return new ProducerRecord<>(
                topic,
                extractPartition(
                        consumedRow,
                        keySerialized,
                        valueSerialized,
                        context.getPartitionsForTopic(topic)),
                readMetadata(consumedRow, WritableMetadata.TIMESTAMP),
                keySerialized,
                valueSerialized,
                headers);
    }

    private Integer extractPartition(
            RowData consumedRow,
            @Nullable byte[] keySerialized,
            byte[] valueSerialized,
            int[] partitions) {
        if (partitioner != null) {
            return partitioner.partition(
                    consumedRow, keySerialized, valueSerialized, topic, partitions);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private <T> T readMetadata(RowData consumedRow, WritableMetadata metadata) {
        final int pos = metadataPositions[metadata.ordinal()];
        if (pos < 0) {
            return null;
        }
        return (T) metadata.converter.read(consumedRow, pos);
    }

    private static RowData createProjectedRow(
            RowData consumedRow, RowKind kind, RowData.FieldGetter[] fieldGetters) {
        final int arity = fieldGetters.length;
        final GenericRowData genericRowData = new GenericRowData(kind, arity);
        for (int fieldPos = 0; fieldPos < arity; fieldPos++) {
            genericRowData.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(consumedRow));
        }
        return genericRowData;
    }

    // --------------------------------------------------------------------------------------------
    // Retract headers
    // --------------------------------------------------------------------------------------------

    private static final KafkaHeader INSERT_HEADER = createRowKindHeader(RowKind.INSERT);
    private static final KafkaHeader UPDATE_BEFORE_HEADER =
            createRowKindHeader(RowKind.UPDATE_BEFORE);
    private static final KafkaHeader UPDATE_AFTER_HEADER =
            createRowKindHeader(RowKind.UPDATE_AFTER);
    private static final KafkaHeader DELETE_HEADER = createRowKindHeader(RowKind.DELETE);

    private static KafkaHeader createRowKindHeader(RowKind kind) {
        return new KafkaHeader(ROW_KIND_HEADER_KEY, new byte[] {kind.toByteValue()});
    }

    private static List<Header> createRowKindOnlyHeaders(RowKind kind) {
        final List<Header> headers = new ArrayList<>(1);
        headers.add(mapRowKindHeader(kind));
        return headers;
    }

    private static KafkaHeader mapRowKindHeader(RowKind kind) {
        switch (kind) {
            case INSERT:
                return INSERT_HEADER;
            case UPDATE_BEFORE:
                return UPDATE_BEFORE_HEADER;
            case UPDATE_AFTER:
                return UPDATE_AFTER_HEADER;
            case DELETE:
                return DELETE_HEADER;
            default:
                throw new UnsupportedOperationException("Unknown row kind: " + kind);
        }
    }
}
