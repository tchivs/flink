/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BinaryInputFormatter}. */
public class BinaryInputFormatterTest {
    @Test
    void testGetRequest() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        BinaryInputFormatter formatter = new BinaryInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args))).isEqualTo("input-text-prompt");
    }

    @Test
    void testGetRequestNull() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("input", "STRING")
                        .column("input2", "FLOAT")
                        .column("input3", "INT")
                        .build();
        BinaryInputFormatter formatter = new BinaryInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt", null, 2};
        // Result is the string, zero bytes for the null, and 4 bytes for the int in little-endian.
        assertThat(formatter.format(args)).isEqualTo("input-text-prompt\2\0\0\0".getBytes());
    }

    @Test
    void testGetRequestArray() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<INT>").build();
        BinaryInputFormatter formatter = new BinaryInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {new Integer[] {1, 2, 3}};
        assertThat(formatter.format(args)).isEqualTo("\1\0\0\0\2\0\0\0\3\0\0\0".getBytes());
    }

    @Test
    void testGetRequestBytes() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "BYTES").build();
        BinaryInputFormatter formatter = new BinaryInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt".getBytes()};
        assertThat(new String(formatter.format(args))).isEqualTo("input-text-prompt");
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
        BinaryInputFormatter formatter = new BinaryInputFormatter(inputSchema.getColumns());
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
        byte[] expected =
                ("input-text-prompt" // STRING
                                + "\1\0\0\0" // INT
                                // DOUBLE IEEE 754 little-endian. Sign=0,Exponent=1024,Mantissa=0
                                + "\0\0\0\0\0\0\0\100"
                                + "\3" // TINYINT
                                + "\4\0" // SMALLINT
                                + "\1" // BOOLEAN
                                + "\5\0\0\0\0\0\0\0" // BIGINT
                                // FLOAT IEEE 754 little-endian. Sign=0,Exponent=129,Mantissa=0.5
                                + "\0\0\300\100"
                                + "a" // CHAR
                                + "b" // VARCHAR
                                + "c" // BINARY not b64 encoded.
                                + "d" // VARBINARY not b64 encoded.
                                + "\107" // DECIMAL unscaled=71
                                + "ab" // ARRAY<STRING>
                                + "\f\0\0\0\1" // ROW
                        )
                        .getBytes("ISO-8859-1"); // NOT UTF-8! We want the raw bytes.
        assertThat(formatter.format(args)).isEqualTo(expected);
    }
}
