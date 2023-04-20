/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.utils.RowDataJsonFormatter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Objects;

/**
 * A wrapper for {@link RowDataJsonFormatter} that satisfies the basic {@link TypeSerializer}
 * interface and serializes {@link RowData} into a UTF-8 JSON string.
 *
 * <p>The serializer assumes that the serialized representation is stored in a bounded data
 * structure (i.e. a byte array), thus, a length header is omitted.
 */
@Confluent
public final class ForegroundResultJsonSerializer extends TypeSerializer<RowData> {

    private final LogicalType consumedType;
    private final ZoneId zoneId;
    private final boolean isInsertOnly;

    private transient RowDataJsonFormatter jsonFormatter;

    public ForegroundResultJsonSerializer(
            LogicalType consumedType, ZoneId zoneId, boolean isInsertOnly) {
        this.consumedType = Preconditions.checkNotNull(consumedType);
        this.zoneId = Preconditions.checkNotNull(zoneId);
        this.isInsertOnly = isInsertOnly;
    }

    private void initFormatter() {
        jsonFormatter =
                new RowDataJsonFormatter(
                        consumedType,
                        zoneId,
                        Thread.currentThread().getContextClassLoader(),
                        isInsertOnly);
    }

    @Override
    public boolean isImmutableType() {
        throw new UnsupportedOperationException(
                "This specialized serializer does not implement isImmutableType().");
    }

    @Override
    public TypeSerializer<RowData> duplicate() {
        return new ForegroundResultJsonSerializer(consumedType, zoneId, isInsertOnly);
    }

    @Override
    public RowData createInstance() {
        throw new UnsupportedOperationException(
                "This specialized serializer does not implement createInstance().");
    }

    @Override
    public RowData copy(RowData from) {
        throw new UnsupportedOperationException(
                "This specialized serializer does not implement copy().");
    }

    @Override
    public RowData copy(RowData from, RowData reuse) {
        throw new UnsupportedOperationException(
                "This specialized serializer does not implement copy().");
    }

    @Override
    public int getLength() {
        throw new UnsupportedOperationException(
                "This specialized serializer does not implement getLength().");
    }

    @Override
    public void serialize(RowData record, DataOutputView target) throws IOException {
        if (jsonFormatter == null) {
            initFormatter();
        }
        final String json = jsonFormatter.convert(record);
        final byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        target.write(bytes);
    }

    @Override
    public RowData deserialize(DataInputView source) {
        throw new UnsupportedOperationException(
                "This specialized serializer does not implement deserialize().");
    }

    @Override
    public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
        throw new UnsupportedOperationException(
                "This specialized serializer does not implement deserialize().");
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {}

    @Override
    public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
        throw new UnsupportedOperationException(
                "This specialized serializer does not implement snapshotConfiguration().");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForegroundResultJsonSerializer that = (ForegroundResultJsonSerializer) o;
        return isInsertOnly == that.isInsertOnly
                && consumedType.equals(that.consumedType)
                && zoneId.equals(that.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumedType, zoneId, isInsertOnly);
    }
}
