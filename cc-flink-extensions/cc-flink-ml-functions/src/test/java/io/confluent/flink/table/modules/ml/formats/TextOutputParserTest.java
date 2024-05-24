/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TextOutputParser}. */
public class TextOutputParserTest {
    @Test
    void testParseResponse() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        TextOutputParser parser = new TextOutputParser(outputSchema.getColumns());
        String response = "\"output-text\"";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[\"output-text\"]");
    }

    @Test
    void testParseResponseMultiline() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        TextOutputParser parser = new TextOutputParser(outputSchema.getColumns());
        String response = "output-text\nmultiline";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text\nmultiline]");
    }

    @Test
    void testParseResponseArray() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<INT>").build();
        assertThatThrownBy(() -> new TextOutputParser(outputSchema.getColumns()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "ML_PREDICT text output only supports a single STRING, CHAR, or VARCHAR type");
    }

    @Test
    void testParseResponseMultiOutput() throws Exception {
        Schema outputSchema =
                Schema.newBuilder()
                        .column("output", "STRING")
                        .column("output2", "STRING")
                        .column("output3", "VARCHAR")
                        .build();
        assertThatThrownBy(() -> new TextOutputParser(outputSchema.getColumns()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "ML_PREDICT text output only supports a single STRING, CHAR, or VARCHAR type");
    }
}
