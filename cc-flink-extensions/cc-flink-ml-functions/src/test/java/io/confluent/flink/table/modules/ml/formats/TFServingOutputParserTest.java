/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;

import io.confluent.flink.table.utils.RemoteRuntimeUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TFServingOutputParser}. */
public class TFServingOutputParserTest {
    @Test
    void testParseResponse() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        TFServingOutputParser parser = new TFServingOutputParser(outputSchema.getColumns());
        String response = "{\"predictions\":[\"output-text\"]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseResponseWrapper() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        TFServingOutputParser parser =
                new TFServingOutputParser(outputSchema.getColumns(), "something");
        String response = "{\"something\":[\"output-text\"]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseResponseWrapperObject() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        TFServingOutputParser parser =
                new TFServingOutputParser(outputSchema.getColumns(), "something");
        String response = "{\"something\":[{\"output\":\"output-text\"}]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseResponseMultiOutput() throws Exception {
        Schema outputSchema =
                Schema.newBuilder()
                        .column("output", "STRING")
                        .column("output2", "INT")
                        .column("output3", "DOUBLE")
                        .column("output4", "TINYINT")
                        .column("output5", "SMALLINT")
                        .column("output6", "BOOLEAN")
                        .column("output8", "BIGINT")
                        .column("output9", "FLOAT")
                        .column("output10", "CHAR")
                        .column("output11", "VARCHAR")
                        .column("output12", "BINARY")
                        .column("output13", "VARBINARY")
                        .column("output14", "DECIMAL(2,1)")
                        .column("output15", "ARRAY<STRING>")
                        .column("output17", "ROW(field1 INT, field2 BOOLEAN)")
                        .build();
        TFServingOutputParser parser = new TFServingOutputParser(outputSchema.getColumns());
        String response =
                "{\"predictions\":[{\"output\":\"output-text\",\"output2\":3,\"output3\":5.0,"
                        + "\"output4\":1,\"output5\":2,\"output6\":true,"
                        + "\"output8\":10000000000,\"output9\":5.0,\"output10\":\"a\","
                        + "\"output11\":\"b\",\"output12\":\"Yw==\",\"output13\":\"ZA==\","
                        + "\"output14\":5.5,\"output15\":[\"a\",\"b\"],"
                        + "\"output17\":{\"field1\":12,\"field2\":true}}]}";
        Row row = parser.parse(RemoteRuntimeUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(15);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
        assertThat(row.getField(1)).isEqualTo(3);
        assertThat(row.getField(2)).isEqualTo(5.0);
        assertThat(row.getField(3)).isEqualTo((byte) 1);
        assertThat(row.getField(4)).isEqualTo((short) 2);
        assertThat(row.getField(5)).isEqualTo(true);
        assertThat(row.getField(6)).isEqualTo(10000000000L);
        assertThat(row.getField(7)).isEqualTo(5.0f);
        assertThat(row.getField(8).toString()).isEqualTo("a");
        assertThat(row.getField(9).toString()).isEqualTo("b");
        assertThat(row.getField(10)).isEqualTo("c".getBytes());
        assertThat(row.getField(11)).isEqualTo("d".getBytes());
        assertThat(row.getField(12)).isEqualTo(new BigDecimal("5.5"));
        assertThat(row.getField(13)).isEqualTo(new String[] {"a", "b"});
        assertThat(row.getField(14)).isEqualTo(Row.of(12, true));
    }
}
