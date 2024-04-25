/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.MlUtils;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TritonOutputParser}. */
public class TritonOutputParserTest {
    @Test
    void testParseResponse() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"BYTES\",\"shape\":[11],\"data\":[\"output-text\"]}]}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseResponseMultiArray() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<ARRAY<INT>>").build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"INT32\",\"shape\":[2,2],\"data\":[[1,2],[3,4]]}]}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[][] {{1, 2}, {3, 4}});
    }

    @Test
    void testParseResponseMultiArrayFlat() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<ARRAY<INT>>").build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"INT32\",\"shape\":[2,2],\"data\":[1,2,3,4]}]}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[][] {{1, 2}, {3, 4}});
    }

    @Test
    void testParseResponseTripleNestedArrayFlat() throws Exception {
        Schema outputSchema =
                Schema.newBuilder().column("output", "ARRAY<ARRAY<ARRAY<INT>>>").build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"INT32\",\"shape\":[2,2,3],\"data\":[1,2,3,4,5,6,7,8,9,10,11,12]}]}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0))
                .isEqualTo(new Integer[][][] {{{1, 2, 3}, {4, 5, 6}}, {{7, 8, 9}, {10, 11, 12}}});
    }

    @Test
    void testParseResponseMultiArrayBinary() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<ARRAY<INT>>").build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"INT32\",\"shape\":[2,2],\"parameters\":{\"binary_data_size\":16}}]}";
        byte[] dataBytes = "\1\0\0\0\2\0\0\0\3\0\0\0\4\0\0\0".getBytes("ISO-8859-1");
        int jsonLength = response.length();
        okhttp3.Headers headers =
                okhttp3.Headers.of(
                        "Content-Type",
                        "application/vnd.sagemaker-triton.binary+json;json-header-size="
                                + String.valueOf(jsonLength));
        byte[] responseBytes = new byte[jsonLength + dataBytes.length];
        System.arraycopy(response.getBytes(), 0, responseBytes, 0, jsonLength);
        System.arraycopy(dataBytes, 0, responseBytes, jsonLength, dataBytes.length);
        Response httpResponse = MlUtils.makeResponse(responseBytes, headers);
        Row row = parser.parse(httpResponse);
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[][] {{1, 2}, {3, 4}});
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
                        .column("output10", "CHAR(2)")
                        .column("output11", "VARCHAR(10)")
                        .column("output12", "BINARY(2)")
                        .column("output13", "VARBINARY(10)")
                        .column("output15", "ARRAY<STRING>")
                        .column("output16", "ARRAY<FLOAT>")
                        .build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":["
                        + "{\"name\":\"output\",\"datatype\":\"BYTES\",\"shape\":[17],\"data\":[\"output-text\"]},"
                        + "{\"name\":\"output2\",\"datatype\":\"INT32\",\"shape\":[1],\"data\":[1]},"
                        + "{\"name\":\"output3\",\"datatype\":\"FP64\",\"shape\":[1],\"data\":[2.0]},"
                        + "{\"name\":\"output4\",\"datatype\":\"INT8\",\"shape\":[1],\"data\":[3]},"
                        + "{\"name\":\"output5\",\"datatype\":\"INT16\",\"shape\":[1],\"data\":[4]},"
                        + "{\"name\":\"output6\",\"datatype\":\"BOOL\",\"shape\":[1],\"data\":[true]},"
                        + "{\"name\":\"output8\",\"datatype\":\"INT64\",\"shape\":[1],\"data\":[5]},"
                        + "{\"name\":\"output9\",\"datatype\":\"FP32\",\"shape\":[1],\"data\":[6.0]},"
                        + "{\"name\":\"output10\",\"datatype\":\"BYTES\",\"shape\":[2],\"data\":[\"ae\"]},"
                        + "{\"name\":\"output11\",\"datatype\":\"BYTES\",\"shape\":[2],\"data\":[\"bf\"]},"
                        + "{\"name\":\"output12\",\"datatype\":\"BYTES\",\"shape\":[2],\"data\":[\"Y2c=\"]},"
                        + "{\"name\":\"output13\",\"datatype\":\"BYTES\",\"shape\":[2],\"data\":[\"ZGg=\"]},"
                        + "{\"name\":\"output15\",\"datatype\":\"BYTES\",\"shape\":[2,2],\"data\":[\"ab\",\"cd\"]},"
                        + "{\"name\":\"output16\",\"datatype\":\"FP32\",\"shape\":[2],\"data\":[1.0,2.0]}]}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(14);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
        assertThat(row.getField(1)).isEqualTo(1);
        assertThat(row.getField(2)).isEqualTo(2.0);
        assertThat(row.getField(3)).isEqualTo((byte) 3);
        assertThat(row.getField(4)).isEqualTo((short) 4);
        assertThat(row.getField(5)).isEqualTo(true);
        assertThat(row.getField(6)).isEqualTo(5L);
        assertThat(row.getField(7)).isEqualTo(6.0f);
        assertThat(row.getField(8).toString()).isEqualTo("ae");
        assertThat(row.getField(9).toString()).isEqualTo("bf");
        assertThat(row.getField(10)).isEqualTo("cg".getBytes());
        assertThat(row.getField(11)).isEqualTo("dh".getBytes());
        assertThat(row.getField(12)).isEqualTo(new String[] {"ab", "cd"});
        assertThat(row.getField(13)).isEqualTo(new Float[] {1.0f, 2.0f});
    }

    @Test
    void testParseResponseMultiOutputBinary() throws Exception {
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
                        .column("output10", "CHAR(2)")
                        .column("output11", "VARCHAR(10)")
                        .column("output12", "BINARY(2)")
                        .column("output13", "VARBINARY(10)")
                        .column("output15", "ARRAY<STRING>")
                        .column("output15a", "ARRAY<STRING>")
                        .column("output16", "ARRAY<FLOAT>")
                        .build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":["
                        + "{\"name\":\"output\",\"datatype\":\"BYTES\",\"shape\":[11],\"parameters\":{\"binary_data_size\":15}},"
                        + "{\"name\":\"output2\",\"datatype\":\"INT32\",\"shape\":[1],\"parameters\":{\"binary_data_size\":4}},"
                        + "{\"name\":\"output3\",\"datatype\":\"FP64\",\"shape\":[1],\"parameters\":{\"binary_data_size\":8}},"
                        + "{\"name\":\"output4\",\"datatype\":\"INT8\",\"shape\":[1],\"parameters\":{\"binary_data_size\":1}},"
                        + "{\"name\":\"output5\",\"datatype\":\"INT16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output6\",\"datatype\":\"BOOL\",\"shape\":[1],\"parameters\":{\"binary_data_size\":1}},"
                        + "{\"name\":\"output8\",\"datatype\":\"INT64\",\"shape\":[1],\"parameters\":{\"binary_data_size\":8}},"
                        + "{\"name\":\"output9\",\"datatype\":\"FP32\",\"shape\":[1],\"parameters\":{\"binary_data_size\":4}},"
                        + "{\"name\":\"output10\",\"datatype\":\"BYTES\",\"shape\":[2],\"parameters\":{\"binary_data_size\":6}},"
                        + "{\"name\":\"output11\",\"datatype\":\"BYTES\",\"shape\":[3],\"parameters\":{\"binary_data_size\":7}},"
                        + "{\"name\":\"output12\",\"datatype\":\"BYTES\",\"shape\":[2],\"parameters\":{\"binary_data_size\":6}},"
                        + "{\"name\":\"output13\",\"datatype\":\"BYTES\",\"shape\":[3],\"parameters\":{\"binary_data_size\":7}},"
                        + "{\"name\":\"output15\",\"datatype\":\"BYTES\",\"shape\":[2,2],\"parameters\":{\"binary_data_size\":12}},"
                        + "{\"name\":\"output15a\",\"datatype\":\"BYTES\",\"shape\":[2,3],\"parameters\":{\"binary_data_size\":13}},"
                        + "{\"name\":\"output16\",\"datatype\":\"FP32\",\"shape\":[2],\"parameters\":{\"binary_data_size\":8}}]}";
        byte[] dataBytes =
                ("\13\0\0\0output-text" // STRING
                                + "\1\0\0\0" // INT 1
                                + "\0\0\0\0\0\0\0\100" // DOUBLE 2.0
                                + "\3" // TINYINT
                                + "\4\0" // SMALLINT
                                + "\1" // BOOLEAN
                                + "\5\0\0\0\0\0\0\0" // BIGINT
                                + "\0\0\300\100" // FLOAT 6.0
                                + "\2\0\0\0ab" // CHAR
                                + "\3\0\0\0cde" // VARCHAR
                                + "\2\0\0\0fg" // BINARY not b64 encoded.
                                + "\3\0\0\0hij" // VARBINARY not b64 encoded.
                                + "\2\0\0\0kl\2\0\0\0mn" // ARRAY<STRING>
                                + "\2\0\0\0op\3\0\0\0qrs" // ARRAY<STRING>
                                + "\0\0\340\100\0\0\0\101" // ARRAY<FLOAT> 7.0 8.0
                        )
                        .getBytes("ISO-8859-1");
        int jsonLength = response.length();
        okhttp3.Headers headers =
                okhttp3.Headers.of("Inference-Header-Content-Length", String.valueOf(jsonLength));
        byte[] responseBytes = new byte[jsonLength + dataBytes.length];
        System.arraycopy(response.getBytes(), 0, responseBytes, 0, jsonLength);
        System.arraycopy(dataBytes, 0, responseBytes, jsonLength, dataBytes.length);
        Response httpResponse = MlUtils.makeResponse(responseBytes, headers);
        Row row = parser.parse(httpResponse);
        assertThat(row.getArity()).isEqualTo(15);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
        assertThat(row.getField(1)).isEqualTo(1);
        assertThat(row.getField(2)).isEqualTo(2.0);
        assertThat(row.getField(3)).isEqualTo((byte) 3);
        assertThat(row.getField(4)).isEqualTo((short) 4);
        assertThat(row.getField(5)).isEqualTo(true);
        assertThat(row.getField(6)).isEqualTo(5L);
        assertThat(row.getField(7)).isEqualTo(6.0f);
        assertThat(row.getField(8).toString()).isEqualTo("ab");
        assertThat(row.getField(9).toString()).isEqualTo("cde");
        assertThat(row.getField(10)).isEqualTo("fg".getBytes());
        assertThat(row.getField(11)).isEqualTo("hij".getBytes());
        assertThat(row.getField(12)).isEqualTo(new String[] {"kl", "mn"});
        assertThat(row.getField(13)).isEqualTo(new String[] {"op", "qrs"});
        assertThat(row.getField(14)).isEqualTo(new Float[] {7.0f, 8.0f});
    }

    @Test
    void testParseResponseIntTooSmall() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "INT").build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"UINT64\",\"shape\":[1],\"data\":[8000000000000000000]}]}";
        assertThatThrownBy(() -> parser.parse(MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("ML Predict found incompatible datatype for output");
    }

    @Test
    void testParseResponseIntOverflow() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "BIGINT").build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"UINT64\",\"shape\":[1],\"data\":[9223372036854775809]}]}";
        assertThatThrownBy(() -> parser.parse(MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "ML Predict could not convert json field to int64, possible overflow");
    }

    @Test
    void testParseResponseIntOverflowBinary() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "BIGINT").build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"UINT64\",\"shape\":[1],\"parameters\":{\"binary_data_size\":8}}]}";
        byte[] dataBytes = "\373\377\377\377\377\377\377\377".getBytes("ISO-8859-1");
        int jsonLength = response.length();
        okhttp3.Headers headers =
                okhttp3.Headers.of("Inference-Header-Content-Length", String.valueOf(jsonLength));
        byte[] responseBytes = new byte[jsonLength + dataBytes.length];
        System.arraycopy(response.getBytes(), 0, responseBytes, 0, jsonLength);
        System.arraycopy(dataBytes, 0, responseBytes, jsonLength, dataBytes.length);
        Response httpResponse = MlUtils.makeResponse(responseBytes, headers);
        assertThatThrownBy(() -> parser.parse(httpResponse))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "ML Predict overflowed when attempting to deserialize a signed 64-bit integer");
    }

    @Test
    void testParseResponseNonstandardTypesBinary() throws Exception {
        Schema outputSchema =
                Schema.newBuilder()
                        .column("output1", "ARRAY<BIGINT>")
                        .column("output2", "ARRAY<BIGINT>")
                        .column("output3", "ARRAY<BIGINT>")
                        .column("output4", "ARRAY<BIGINT>")
                        .column("output5", "ARRAY<BIGINT>")
                        .column("output6", "ARRAY<BIGINT>")
                        .column("output7", "ARRAY<FLOAT>")
                        .column("output8", "ARRAY<FLOAT>")
                        .build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":["
                        + "{\"name\":\"output1\",\"datatype\":\"INT8\",\"shape\":[2],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output2\",\"datatype\":\"INT16\",\"shape\":[2],\"parameters\":{\"binary_data_size\":4}},"
                        + "{\"name\":\"output3\",\"datatype\":\"UINT8\",\"shape\":[2],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output4\",\"datatype\":\"UINT16\",\"shape\":[2],\"parameters\":{\"binary_data_size\":4}},"
                        + "{\"name\":\"output5\",\"datatype\":\"UINT32\",\"shape\":[2],\"parameters\":{\"binary_data_size\":8}},"
                        + "{\"name\":\"output6\",\"datatype\":\"UINT64\",\"shape\":[2],\"parameters\":{\"binary_data_size\":16}},"
                        + "{\"name\":\"output7\",\"datatype\":\"FP16\",\"shape\":[2],\"parameters\":{\"binary_data_size\":4}},"
                        + "{\"name\":\"output8\",\"datatype\":\"BF16\",\"shape\":[2],\"parameters\":{\"binary_data_size\":4}}]}";
        byte[] dataBytes =
                ("\1\2" // INT8 1,2
                                + "\3\0\4\0" // INT16 3,4
                                + "\201\202" // UINT8 129,130
                                + "\0\201\1\201" // UINT16 33024,33025
                                + "\0\0\0\201\1\0\0\201" // UINT32, overflow 2164260864,2164260865
                                + "\5\0\0\0\0\0\0\0\373\377\377\377\377\377\377\0" // UINT64,
                                // 5,something
                                // big
                                + "\0\106\0\107" // IEEE half-precision FLOAT 6.0,7.0
                                + "\0\101\20\101" // bfloat16 8.0, 9.0
                        )
                        .getBytes("ISO-8859-1");
        int jsonLength = response.length();
        okhttp3.Headers headers =
                okhttp3.Headers.of("Inference-Header-Content-Length", String.valueOf(jsonLength));
        byte[] responseBytes = new byte[jsonLength + dataBytes.length];
        System.arraycopy(response.getBytes(), 0, responseBytes, 0, jsonLength);
        System.arraycopy(dataBytes, 0, responseBytes, jsonLength, dataBytes.length);
        Response httpResponse = MlUtils.makeResponse(responseBytes, headers);
        Row row = parser.parse(httpResponse);
        assertThat(row.getArity()).isEqualTo(8);
        assertThat(row.getField(0)).isEqualTo(new Long[] {1L, 2L});
        assertThat(row.getField(1)).isEqualTo(new Long[] {3L, 4L});
        assertThat(row.getField(2)).isEqualTo(new Long[] {129L, 130L});
        assertThat(row.getField(3)).isEqualTo(new Long[] {33024L, 33025L});
        assertThat(row.getField(4)).isEqualTo(new Long[] {2164260864L, 2164260865L});
        assertThat(row.getField(5)).isEqualTo(new Long[] {5L, 72057594037927931L});
        assertThat(row.getField(6)).isEqualTo(new Float[] {6.0f, 7.0f});
        assertThat(row.getField(7)).isEqualTo(new Float[] {8.0f, 9.0f});
    }

    @Test
    void testParseResponseHalfPrecisionFloats() throws Exception {
        Schema outputSchema =
                Schema.newBuilder()
                        .column("output", "FLOAT")
                        .column("output2", "FLOAT")
                        .column("output3", "FLOAT")
                        .column("output4", "FLOAT")
                        .column("output5", "FLOAT")
                        .column("output6", "FLOAT")
                        .column("output7", "FLOAT")
                        .column("output8", "FLOAT")
                        .column("output9", "FLOAT")
                        .column("output10", "FLOAT")
                        .build();
        TritonOutputParser parser = new TritonOutputParser(outputSchema.getColumns());
        String response =
                "{\"outputs\":[{\"name\":\"output\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output2\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output3\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output4\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output5\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output6\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output7\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output8\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output9\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}},"
                        + "{\"name\":\"output10\",\"datatype\":\"FP16\",\"shape\":[1],\"parameters\":{\"binary_data_size\":2}}]}";

        byte[] dataBytes =
                ("\110\102" // pi
                                + "\110\302" // -pi
                                + "\0\374" // -Inf
                                + "\0\174" // +Inf
                                + "\1\174" // NaN
                                + "\2\174" // another NaN
                                + "\0\100" // two
                                + "\110\202" // -0.00003481
                                + "\1\0" // 5.98e-8
                                + "\0\2" // 0.00003052
                        )
                        .getBytes("ISO-8859-1");
        int jsonLength = response.length();
        okhttp3.Headers headers =
                okhttp3.Headers.of("Inference-Header-Content-Length", String.valueOf(jsonLength));
        byte[] responseBytes = new byte[jsonLength + dataBytes.length];
        System.arraycopy(response.getBytes(), 0, responseBytes, 0, jsonLength);
        System.arraycopy(dataBytes, 0, responseBytes, jsonLength, dataBytes.length);
        Response httpResponse = MlUtils.makeResponse(responseBytes, headers);
        Row row = parser.parse(httpResponse);
        assertThat(row.getArity()).isEqualTo(10);
        assertThat(row.getField(0)).isEqualTo(3.140625f);
        assertThat(row.getField(1)).isEqualTo(-3.140625f);
        assertThat(row.getField(2)).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat(row.getField(3)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(row.getField(4)).isEqualTo(Float.NaN);
        assertThat(row.getField(5)).isEqualTo(Float.NaN);
        assertThat(row.getField(6)).isEqualTo(2.0f);
        assertThat(row.getField(7)).isEqualTo(-0.000034809113f);
        assertThat(row.getField(8)).isEqualTo(5.9604645e-8f);
        assertThat(row.getField(9)).isEqualTo(0.000030517578f);
    }
}
