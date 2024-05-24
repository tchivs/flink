/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TritonInputFormatter}. */
public class TritonInputFormatterTest {
    @Test
    void testGetRequest() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        TritonInputFormatter formatter =
                new TritonInputFormatter(inputSchema.getColumns(), outputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"inputs\":[{\"name\":\"input\",\"datatype\":\"BYTES\",\"shape\":[17],\"data\":[\"input-text-prompt\"]}]}");
    }

    @Test
    void testGetRequestMultiArray() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<ARRAY<INT>>").build();
        Schema outputSchema =
                Schema.newBuilder().column("output1", "STRING").column("output2", "BYTES").build();
        TritonInputFormatter formatter =
                new TritonInputFormatter(inputSchema.getColumns(), outputSchema.getColumns());
        Object[] args =
                new Object[] {
                    new Integer[][] {{1, 2}, {3, 4}},
                };
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"parameters\":{\"binary_data_output\":true},"
                                + "\"inputs\":[{\"name\":\"input\",\"datatype\":\"INT32\",\"shape\":[2,2],\"data\":[[1,2],[3,4]]}]}");
    }

    @Test
    void testGetRequestMultiArrayPrimative() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<ARRAY<INT>>").build();
        Schema outputSchema =
                Schema.newBuilder().column("output1", "STRING").column("output2", "BYTES").build();
        TritonInputFormatter formatter =
                new TritonInputFormatter(inputSchema.getColumns(), outputSchema.getColumns());
        Object[] args =
                new Object[] {
                    new int[][] {{1, 2}, {3, 4}},
                };
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"parameters\":{\"binary_data_output\":true},"
                                + "\"inputs\":[{\"name\":\"input\",\"datatype\":\"INT32\",\"shape\":[2,2],\"data\":[[1,2],[3,4]]}]}");
    }

    @Test
    void testGetRequestMultiArrayList() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<ARRAY<INT>>").build();
        Schema outputSchema =
                Schema.newBuilder().column("output1", "STRING").column("output2", "BYTES").build();
        TritonInputFormatter formatter =
                new TritonInputFormatter(inputSchema.getColumns(), outputSchema.getColumns());
        Object[] args =
                new Object[] {
                    new ArrayList<List<Integer>>() {
                        {
                            add(
                                    new ArrayList<Integer>() {
                                        {
                                            add(1);
                                            add(2);
                                        }
                                    });
                            add(
                                    new ArrayList<Integer>() {
                                        {
                                            add(3);
                                            add(4);
                                        }
                                    });
                        }
                    }
                };
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"parameters\":{\"binary_data_output\":true},"
                                + "\"inputs\":[{\"name\":\"input\",\"datatype\":\"INT32\",\"shape\":[2,2],\"data\":[[1,2],[3,4]]}]}");
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
                        .column("input15", "ARRAY<STRING>")
                        .column("input16", "ARRAY<BYTES>")
                        .column("input17", "ARRAY<FLOAT>")
                        .build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        TritonInputFormatter formatter =
                new TritonInputFormatter(inputSchema.getColumns(), outputSchema.getColumns());
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
                    "ae", // CHAR
                    "bf", // VARCHAR
                    "cg".getBytes(), // BINARY
                    "dhx".getBytes(), // VARBINARY
                    new String[] {"ab", "cd"}, // ARRAY<STRING>
                    new byte[][] {"ijz".getBytes(), "kl".getBytes()}, // ARRAY<BYTES>
                    new Float[] {1.0f, 2.0f} // ARRAY<FLOAT>
                };
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"inputs\":["
                                + "{\"name\":\"input\",\"datatype\":\"BYTES\",\"shape\":[17],\"data\":[\"input-text-prompt\"]},"
                                + "{\"name\":\"input2\",\"datatype\":\"INT32\",\"shape\":[1],\"data\":[1]},"
                                + "{\"name\":\"input3\",\"datatype\":\"FP64\",\"shape\":[1],\"data\":[2.0]},"
                                + "{\"name\":\"input4\",\"datatype\":\"INT8\",\"shape\":[1],\"data\":[3]},"
                                + "{\"name\":\"input5\",\"datatype\":\"INT16\",\"shape\":[1],\"data\":[4]},"
                                + "{\"name\":\"input6\",\"datatype\":\"BOOL\",\"shape\":[1],\"data\":[true]},"
                                + "{\"name\":\"input8\",\"datatype\":\"INT64\",\"shape\":[1],\"data\":[5]},"
                                + "{\"name\":\"input9\",\"datatype\":\"FP32\",\"shape\":[1],\"data\":[6.0]},"
                                + "{\"name\":\"input10\",\"datatype\":\"BYTES\",\"shape\":[2],\"data\":[\"ae\"]},"
                                + "{\"name\":\"input11\",\"datatype\":\"BYTES\",\"shape\":[2],\"data\":[\"bf\"]},"
                                + "{\"name\":\"input12\",\"datatype\":\"BYTES\",\"shape\":[2],\"parameters\":{\"binary_data_size\":6}},"
                                + "{\"name\":\"input13\",\"datatype\":\"BYTES\",\"shape\":[3],\"parameters\":{\"binary_data_size\":7}},"
                                + "{\"name\":\"input15\",\"datatype\":\"BYTES\",\"shape\":[2,2],\"data\":[\"ab\",\"cd\"]},"
                                + "{\"name\":\"input16\",\"datatype\":\"BYTES\",\"shape\":[2,3],\"parameters\":{\"binary_data_size\":13}},"
                                + "{\"name\":\"input17\",\"datatype\":\"FP32\",\"shape\":[2],\"data\":[1.0,2.0]}]}"
                                + "\2\0\0\0cg\3\0\0\0dhx\3\0\0\0ijz\2\0\0\0kl");
    }
}
