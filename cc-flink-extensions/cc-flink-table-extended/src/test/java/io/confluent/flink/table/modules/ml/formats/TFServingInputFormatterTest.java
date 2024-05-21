/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TFServingInputFormatter}. */
public class TFServingInputFormatterTest {
    @Test
    void testGetRequest() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        TFServingInputFormatter formatter = new TFServingInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo("{\"instances\":[\"input-text-prompt\"]}");
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
        TFServingInputFormatter formatter = new TFServingInputFormatter(inputSchema.getColumns());
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
                        "{\"instances\":[{\"input\":\"input-text-prompt\",\"input2\":1,"
                                + "\"input3\":2.0,\"input4\":3,\"input5\":4,\"input6\":true,"
                                + "\"input8\":5,\"input9\":6.0,\"input10\":\"a\",\"input11\":\"b\","
                                + "\"input12_bytes\":{\"b64\":\"Yw==\"},\"input13_bytes\":{\"b64\":\"ZA==\"},\"input14\":7.1,"
                                + "\"input15\":[\"a\",\"b\"],\"input17\":{\"field1\":12,\"field2\":true}}]}");
    }

    @Test
    void testGetRequestWithWrapper() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("input", "STRING")
                        .column("input2", "INT")
                        .column("input3", "VARBINARY")
                        .build();
        TFServingInputFormatter formatter =
                new TFServingInputFormatter(inputSchema.getColumns(), "data_foo");
        Object[] args = new Object[] {"input-text-prompt", 1, "c".getBytes()};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"instances\":[{\"data_foo\":{\"input\":\"input-text-prompt\","
                                + "\"input2\":1,\"input3_bytes\":{\"b64\":\"Yw==\"}}}]}");
    }
}
