/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForegroundResultJsonSerializer}. */
@Confluent
public class ForegroundResultJsonSerializerTest {

    @Test
    public void testSerialization() throws IOException {
        final GenericRowData row = new GenericRowData(3);
        row.setRowKind(RowKind.UPDATE_BEFORE);
        row.setField(0, 42);
        row.setField(1, StringData.fromString("Hello world!"));
        row.setField(2, null);

        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.INT(), DataTypes.STRING(), DataTypes.ARRAY(DataTypes.BIGINT()));

        final TypeSerializer<RowData> serializer =
                new ForegroundResultJsonSerializer(
                        dataType.getLogicalType(), ZoneId.of("UTC"), false);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
        serializer.serialize(row, wrapper);

        assertThat(new String(baos.toByteArray(), StandardCharsets.UTF_8))
                .isEqualTo("{\"op\":1,\"row\":[\"42\",\"Hello world!\",null]}");
    }
}
