/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TextInputFormatter}. */
public class TextInputFormatterTest {
    @Test
    void testGetRequest() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        TextInputFormatter formatter = new TextInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args))).isEqualTo("input-text-prompt");
    }

    @Test
    void testGetRequestArray() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<INT>").build();
        assertThatThrownBy(() -> new TextInputFormatter(inputSchema.getColumns()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "ML_PREDICT text input only accepts STRING, CHAR, or VARCHAR types");
    }

    @Test
    void testGetRequestMultiInput() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("input", "STRING")
                        .column("input2", "VARCHAR")
                        .column("input3", "CHAR")
                        .build();
        TextInputFormatter formatter = new TextInputFormatter(inputSchema.getColumns());
        Object[] args = new Object[] {"input-text-prompt\nmultiline", "a", "b"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo("input-text-prompt\nmultiline\na\nb");
    }
}
