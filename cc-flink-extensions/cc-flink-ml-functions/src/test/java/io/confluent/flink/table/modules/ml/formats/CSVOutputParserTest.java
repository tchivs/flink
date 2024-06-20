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

/** Tests for {@link CSVOutputParser}. */
public class CSVOutputParserTest {
    @Test
    void testParseResponse() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CSVOutputParser parser = new CSVOutputParser(outputSchema.getColumns());
        String response = "\"output-text\"";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseResponseArray() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<INT>").build();
        CSVOutputParser parser = new CSVOutputParser(outputSchema.getColumns());
        String response = "1,2,3";
        Row row = parser.parse(RemoteRuntimeUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[] {1, 2, 3});
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
                        .build();
        CSVOutputParser parser = new CSVOutputParser(outputSchema.getColumns());
        String response =
                "\"output-text-prompt\",1,2.0,3,4,true,5,6.0,\"a\",\"b\",\"Yw==\",\"ZA==\",7.1";
        Row row = parser.parse(RemoteRuntimeUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(13);
        assertThat(row.getField(0).toString()).isEqualTo("output-text-prompt");
        assertThat(row.getField(1)).isEqualTo(1);
        assertThat(row.getField(2)).isEqualTo(2.0);
        assertThat(row.getField(3)).isEqualTo((byte) 3);
        assertThat(row.getField(4)).isEqualTo((short) 4);
        assertThat(row.getField(5)).isEqualTo(true);
        assertThat(row.getField(6)).isEqualTo(5L);
        assertThat(row.getField(7)).isEqualTo(6.0f);
        assertThat(row.getField(8).toString()).isEqualTo("a");
        assertThat(row.getField(9).toString()).isEqualTo("b");
        assertThat(row.getField(10)).isEqualTo("c".getBytes());
        assertThat(row.getField(11)).isEqualTo("d".getBytes());
        assertThat(row.getField(12)).isEqualTo(new BigDecimal("7.1"));
    }
}
