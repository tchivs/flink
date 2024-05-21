/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonArrayInputFormatter}. */
public class JsonArrayInputFormatterTest {
    @Test
    void testGetRequest() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        JsonArrayInputFormatter formatter = new JsonArrayInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args))).isEqualTo("[\"input-text-prompt\"]");
    }

    @Test
    void testGetRequestArray() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<INT>").build();
        JsonArrayInputFormatter formatter = new JsonArrayInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {new Integer[] {1, 2, 3}};
        assertThat(new String(formatter.format(args))).isEqualTo("[1,2,3]");
    }

    @Test
    void testGetRequestMultiArray() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<ARRAY<INT>>").build();
        JsonArrayInputFormatter formatter = new JsonArrayInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {new Integer[][] {{1, 2, 3}, {4, 5, 6}}};
        assertThat(new String(formatter.format(args))).isEqualTo("[[1,2,3],[4,5,6]]");
    }

    @Test
    void testGetRequestMultiInput() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("input", "STRING")
                        .column("input2", "INT")
                        .column("input3", "DOUBLE")
                        .column("input4", "TINYINT")
                        .column("input5", "SMALLINT")
                        .column("input6", "BOOLEAN")
                        .column("input8", "BIGINT")
                        .column("input9", "FLOAT")
                        .column("input10", "CHAR")
                        .column("input11", "VARCHAR")
                        .column("input12", "BINARY")
                        .column("input13", "VARBINARY")
                        .column("input14", "DECIMAL")
                        .column("input15", "ARRAY<STRING>")
                        .column("input17", "ROW(field1 INT, field2 BOOLEAN)")
                        .build();
        JsonArrayInputFormatter formatter = new JsonArrayInputFormatter(inputSchema.getColumns());
        Object[] args =
                new Object[] {
                    "input-text-prompt",
                    1, // INT
                    2.0, // DOUBLE
                    (byte) 3, // TINYINT
                    (short) 4, // SMALLINT
                    true, // BOOLEAN
                    5L, // BIGINT
                    6.0f, // FLOAT
                    "a", // CHAR
                    "b", // VARCHAR
                    "c".getBytes(), // BINARY
                    "d".getBytes(), // VARBINARY
                    new BigDecimal("7.1"), // DECIMAL
                    new String[] {"a", "b"}, // ARRAY<STRING>
                    Row.of(12, true)
                };
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "[\"input-text-prompt\",1,2.0,3,4,true,5,6.0,\"a\",\"b\",\"Yw==\","
                                + "\"ZA==\",7.1,[\"a\",\"b\"],{\"field1\":12,\"field2\":true}]");
    }
}
