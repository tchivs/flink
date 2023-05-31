/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A specific {@link KafkaDeserializationSchema} for {@link ConfluentManagedTableSource}. It is
 * responsible for projection of keys and metadata columns and most importantly deserializing
 * changes into the changelog.
 *
 * <p>It takes a {@link ManagedChangelogMode} parameter. The parameter is not the table mode but the
 * mode of the deserialization schema. It decides about how to read Kafka records into {@link
 * RowData}:
 *
 * <ul>
 *   <li>{@link ManagedChangelogMode#APPEND} means that the value is treated "as is". This means
 *       that the decoding format is responsible for the change flag. And could potentially "append"
 *       any kind of change.
 *   <li>{@link ManagedChangelogMode#RETRACT} uses a byte flag in the header for all kinds of
 *       changes. Insertion if no header is present.
 *   <li>{@link ManagedChangelogMode#UPSERT} uses null in the value for deletions. Otherwise update
 *       after.
 * </ul>
 */
@Confluent
public class ConfluentManagedKafkaDeserializationSchema
        implements KafkaRecordDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private static final String ROW_KIND_HEADER_KEY = "op";

    private final @Nullable DeserializationSchema<RowData> keyDeserialization;

    private final DeserializationSchema<RowData> valueDeserialization;

    private final boolean hasMetadata;

    private final BufferingCollector keyCollector;

    private final OutputProjectionCollector outputCollector;

    private final TypeInformation<RowData> producedTypeInfo;

    private final ManagedChangelogMode changelogMode;

    ConfluentManagedKafkaDeserializationSchema(
            int physicalArity,
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            int[] keyProjection,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            ManagedChangelogMode changelogMode) {
        if (changelogMode == ManagedChangelogMode.UPSERT) {
            Preconditions.checkArgument(
                    keyDeserialization != null && keyProjection.length > 0,
                    "Key must be set in upsert mode for deserialization schema.");
        }
        this.keyDeserialization = keyDeserialization;
        this.valueDeserialization = valueDeserialization;
        this.hasMetadata = hasMetadata;
        this.keyCollector = new BufferingCollector();
        this.outputCollector =
                new OutputProjectionCollector(
                        physicalArity,
                        keyProjection,
                        valueProjection,
                        metadataConverters,
                        changelogMode);
        this.producedTypeInfo = producedTypeInfo;
        this.changelogMode = changelogMode;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        if (keyDeserialization != null) {
            keyDeserialization.open(context);
        }
        valueDeserialization.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector)
            throws IOException {
        // shortcut in case no output projection is required,
        // also not for a cartesian product with the keys
        if (changelogMode == ManagedChangelogMode.APPEND
                && keyDeserialization == null
                && !hasMetadata) {
            valueDeserialization.deserialize(record.value(), collector);
            return;
        }

        // buffer key(s)
        if (keyDeserialization != null) {
            keyDeserialization.deserialize(record.key(), keyCollector);
        }

        // project output while emitting values
        outputCollector.inputRecord = record;
        outputCollector.physicalKeyRows = keyCollector.buffer;
        outputCollector.outputCollector = collector;
        if (record.value() == null && changelogMode == ManagedChangelogMode.UPSERT) {
            // collect tombstone messages in upsert mode by hand
            outputCollector.collect(null);
        } else {
            valueDeserialization.deserialize(record.value(), outputCollector);
        }
        keyCollector.buffer.clear();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    // --------------------------------------------------------------------------------------------

    interface MetadataConverter extends Serializable {
        Object read(ConsumerRecord<?, ?> record, boolean isRetract);
    }

    // --------------------------------------------------------------------------------------------

    private static final class BufferingCollector implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final List<RowData> buffer = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            buffer.add(record);
        }

        @Override
        public void close() {
            // nothing to do
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Emits a row with key, value, and metadata fields.
     *
     * <p>The collector is able to handle the following kinds of keys:
     *
     * <ul>
     *   <li>No key is used.
     *   <li>A key is used.
     *   <li>The deserialization schema emits multiple keys.
     *   <li>Keys and values have overlapping fields.
     *   <li>Keys are used and value is null.
     * </ul>
     */
    private static final class OutputProjectionCollector
            implements Collector<RowData>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int physicalArity;

        private final int[] keyProjection;

        private final int[] valueProjection;

        private final MetadataConverter[] metadataConverters;

        private final ManagedChangelogMode changelogMode;

        private transient ConsumerRecord<?, ?> inputRecord;

        private transient List<RowData> physicalKeyRows;

        private transient Collector<RowData> outputCollector;

        OutputProjectionCollector(
                int physicalArity,
                int[] keyProjection,
                int[] valueProjection,
                MetadataConverter[] metadataConverters,
                ManagedChangelogMode changelogMode) {
            this.physicalArity = physicalArity;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.metadataConverters = metadataConverters;
            this.changelogMode = changelogMode;
        }

        @Override
        public void collect(RowData physicalValueRow) {
            // no key defined
            if (keyProjection.length == 0) {
                emitRow(null, (GenericRowData) physicalValueRow);
                return;
            }

            // otherwise emit a value for each key
            for (RowData physicalKeyRow : physicalKeyRows) {
                emitRow((GenericRowData) physicalKeyRow, (GenericRowData) physicalValueRow);
            }
        }

        @Override
        public void close() {
            // nothing to do
        }

        private void emitRow(
                @Nullable GenericRowData physicalKeyRow,
                @Nullable GenericRowData physicalValueRow) {
            final RowKind rowKind;
            if (physicalValueRow == null) {
                if (changelogMode == ManagedChangelogMode.UPSERT) {
                    rowKind = RowKind.DELETE;
                } else {
                    throw new DeserializationException(
                            "Invalid null value received in non-upsert mode. Could not to set row kind for output record.");
                }
            } else if (changelogMode == ManagedChangelogMode.RETRACT) {
                final Header header = inputRecord.headers().lastHeader(ROW_KIND_HEADER_KEY);
                // INSERT by default
                byte op = 0;
                if (header != null) {
                    final byte[] headerBytes = header.value();
                    if (headerBytes.length == 1) {
                        op = headerBytes[0];
                    }
                }
                rowKind = RowKind.fromByteValue(op);
            } else if (changelogMode == ManagedChangelogMode.UPSERT) {
                rowKind = RowKind.UPDATE_AFTER;
            } else {
                rowKind = physicalValueRow.getRowKind();
            }

            final int metadataArity = metadataConverters.length;
            final GenericRowData producedRow =
                    new GenericRowData(rowKind, physicalArity + metadataArity);

            for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
                assert physicalKeyRow != null;
                producedRow.setField(keyProjection[keyPos], physicalKeyRow.getField(keyPos));
            }

            if (physicalValueRow != null) {
                for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
                    producedRow.setField(
                            valueProjection[valuePos], physicalValueRow.getField(valuePos));
                }
            }

            final boolean isRetract = changelogMode == ManagedChangelogMode.RETRACT;
            for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
                producedRow.setField(
                        physicalArity + metadataPos,
                        metadataConverters[metadataPos].read(inputRecord, isRetract));
            }

            outputCollector.collect(producedRow);
        }
    }
}
