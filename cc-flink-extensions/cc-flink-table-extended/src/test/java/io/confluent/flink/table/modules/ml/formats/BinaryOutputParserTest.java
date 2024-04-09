/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.MlUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BinaryOutputParser}. */
public class BinaryOutputParserTest {
    @Test
    void testParseResponseArray() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<INT>").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\1\0\0\0\2\0\0\0\3\0\0\0";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[] {1, 2, 3});
    }

    @Test
    void testParseResponseBoolean() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "BOOLEAN").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\1";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(true);
    }

    @Test
    void testParseResponseByte() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "TINYINT").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\1";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo((byte) 1);
    }

    @Test
    void testParseResponseShort() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "SMALLINT").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\1\0";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo((short) 1);
    }

    @Test
    void testParseResponseInt() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "INT").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\1\0\0\0";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(1);
    }

    @Test
    void testParseResponseLong() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "BIGINT").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\1\0\0\0\0\0\0\0";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(1L);
    }

    @Test
    void testParseResponseFloat() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "FLOAT").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\0\0\40\101";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(10.0f);
    }

    @Test
    void testParseResponseDouble() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "DOUBLE").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\0\0\0\0\0\0$@";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(10.0);
    }

    @Test
    void testParseResponseString() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "output-text";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @Test
    void testParseResponseBytes() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "BYTES").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        byte[] bytes = "output-text".getBytes();
        Row row = parser.parse(MlUtils.makeResponse(new String(bytes)));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo("output-text".getBytes());
    }

    @Test
    void testParseResponseDecimal() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "DECIMAL(2,1)").build();
        BinaryOutputParser parser = new BinaryOutputParser(outputSchema.getColumns());
        String response = "\71\0\0\0\0\0\0\0";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new BigDecimal("5.7"));
    }

    @Test
    void testParseResponseArrayString() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<STRING>").build();
        assertThatThrownBy(() -> new BinaryOutputParser(outputSchema.getColumns()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("For binary output, array types are only supported if");
    }
}
