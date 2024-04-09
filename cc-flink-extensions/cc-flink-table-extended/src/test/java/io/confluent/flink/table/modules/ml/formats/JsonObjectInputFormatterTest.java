/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonObjectInputFormatter}. */
public class JsonObjectInputFormatterTest {
    @Test
    void testGetRequest() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        JsonObjectInputFormatter formatter = new JsonObjectInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo("{\"input\":\"input-text-prompt\"}");
    }

    void testGetRequestWithWrapper() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        JsonObjectInputFormatter formatter =
                new JsonObjectInputFormatter(inputSchema.getColumns(), "wrapper");
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(formatter.format(args))
                .isEqualTo("{\"wrapper\":{\"input\":\"input-text-prompt\"}}");
    }

    @Test
    void testGetRequestArray() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<INT>").build();
        JsonObjectInputFormatter formatter = new JsonObjectInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {new Integer[] {1, 2, 3}};
        assertThat(new String(formatter.format(args))).isEqualTo("{\"input\":[1,2,3]}");
    }

    @Test
    void testGetRequestMultiArray() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<ARRAY<INT>>").build();
        JsonObjectInputFormatter formatter = new JsonObjectInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {new Integer[][] {{1, 2, 3}, {4, 5, 6}}};
        assertThat(new String(formatter.format(args))).isEqualTo("{\"input\":[[1,2,3],[4,5,6]]}");
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
                        .column("input16", "ARRAY<ARRAY<INT>>")
                        .column("input17", "ROW(field1 INT, field2 BOOLEAN)")
                        .build();
        JsonObjectInputFormatter formatter = new JsonObjectInputFormatter(inputSchema.getColumns());
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
                    new ArrayList<String>() {
                        {
                            add("a");
                            add("b");
                        }
                    }, // ARRAY<STRING>
                    new int[][] {{1, 2, 3}, {4, 5, 6}}, // ARRAY<ARRAY<INT>>
                    Row.of(12, true)
                };
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"input\":\"input-text-prompt\",\"input2\":1,\"input3\":2.0,"
                                + "\"input4\":3,\"input5\":4,\"input6\":true,\"input8\":5,"
                                + "\"input9\":6.0,\"input10\":\"a\",\"input11\":\"b\","
                                + "\"input12\":\"Yw==\",\"input13\":\"ZA==\",\"input14\":7.1,"
                                + "\"input15\":[\"a\",\"b\"],\"input16\":[[1,2,3],[4,5,6]],"
                                + "\"input17\":{\"field1\":12,\"field2\":true}}");
    }
}
