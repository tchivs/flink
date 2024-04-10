/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogModel.ModelTask;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.MlUtils;
import okhttp3.Request;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for SagemakerProvider. */
public class SagemakerProviderTest {

    @Test
    void testBadEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("SAGEMAKER.ENDPOINT", "fake-endpoint");
        assertThatThrownBy(() -> new SagemakerProvider(model))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to be a valid URL");
    }

    @Test
    void testLocalhostEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("SAGEMAKER.ENDPOINT", "https://localhost/wrong");
        assertThatThrownBy(() -> new SagemakerProvider(model))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testWrongEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("SAGEMAKER.ENDPOINT", "https://fake-endpoint.com/wrong");
        assertThatThrownBy(() -> new SagemakerProvider(model))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testGetRequest() throws Exception {
        CatalogModel model = getCatalogModel();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = sagemakerProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://runtime.sagemaker.us-west-2.amazonaws.com/endpoints/1234/invocations/");
        assertThat(request.method()).isEqualTo("POST");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("input-text-prompt");
    }

    @Test
    void testGetRequestMultiInput() throws Exception {
        CatalogModel model = getCatalogModelMultiType();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
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
                    Row.of(12, true)
                };
        Request request = sagemakerProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://runtime.sagemaker.us-west-2.amazonaws.com/endpoints/1234/invocations/");
        assertThat(request.method()).isEqualTo("POST");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo("input-text-prompt,1,2.0,3,4,true,5,6.0,a,b,Yw==," + "ZA==,7.1,12,true");
    }

    @Test
    void testBadResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                sagemakerProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("No predictions found in ML Predict response");
    }

    @Test
    void testErrorResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        String response = "{\"ErrorCode\":[\"Model not found or something.\"]}";
        assertThatThrownBy(
                        () ->
                                sagemakerProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Model not found or something");
    }

    @Test
    void testParseResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"predictions\":[\"output-text\"]}";
        Row row = sagemakerProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @Test
    void testParseResponseWithObject() throws Exception {
        CatalogModel model = getCatalogModel();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"predictions\":[{\"output\":\"output-text\"}]}";
        Row row = sagemakerProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @Test
    void testParseResponseExtraOutput() throws Exception {
        CatalogModel model = getCatalogModel();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"predictions\":[{\"output\":\"output-text\",\"score\":0.9}]}";
        Row row = sagemakerProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @Test
    void testParseResponseMissingOutput() throws Exception {
        CatalogModel model = getCatalogModel();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"predictions\":[{\"not-output\":\"output-text\",\"score\":0.9}]}";
        assertThatThrownBy(
                        () ->
                                sagemakerProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Field output not found in remote ML Prediction response");
    }

    @Test
    void testParseResponseMultiType() throws Exception {
        CatalogModel model = getCatalogModelMultiType();
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response =
                "{\"predictions\":[{\"output\":\"output-text\",\"output2\":3,\"output3\":5.0,"
                        + "\"output4\":1,\"output5\":2,\"output6\":true,\"output7\":\"2021-01-01\","
                        + "\"output8\":10000000000,\"output9\":5.0,\"output10\":\"a\","
                        + "\"output11\":\"b\",\"output12\":\"Yg==\",\"output13\":\"Yg==\","
                        + "\"output14\":5.5,\"output15\":[1.0,2.0],"
                        + "\"output17\":{\"field1\":1,\"field2\":true}}]}";
        Row row = sagemakerProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
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
        assertThat(row.getField(10)).isEqualTo(new byte[] {98});
        assertThat(row.getField(11)).isEqualTo(new byte[] {98});
        assertThat(row.getField(12)).isEqualTo(new BigDecimal("5.5"));
        assertThat(row.getField(13)).isEqualTo(new Double[] {1.0, 2.0});
        assertThat(row.getField(14)).isEqualTo(Row.of(1, true));
    }

    @Test
    void testGetRequestTritonMixedBinary() throws Exception {
        Request request = getRequestBinaryWithFormat("triton");
        // Content type should include the length of the json part.
        assertThat(request.body().contentType().toString())
                .isEqualTo("application/vnd.sagemaker-triton.binary+json;json-header-size=160");
        String expectedJson =
                "{\"inputs\":["
                        + "{\"name\":\"input\",\"datatype\":\"INT32\",\"shape\":[3],\"data\":[1,2,3]},"
                        + "{\"name\":\"input2\",\"datatype\":\"BYTES\",\"shape\":[3],"
                        + "\"parameters\":{\"binary_data_size\":7}}]}";

        int expectedJsonLength =
                Integer.parseInt(request.body().contentType().parameter("json-header-size"));
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        byte[] bytes = buffer.readByteArray();
        assertThat(bytes.length).isEqualTo(expectedJsonLength + 4 + "abc".length());
        assertThat(new String(bytes, 0, expectedJsonLength)).isEqualTo(expectedJson);
        // binary should be next, 4 bytes of length and then the binary data.
        byte[] binary = new byte[7];
        System.arraycopy(bytes, expectedJsonLength, binary, 0, 7);
        assertThat(binary).isEqualTo(new byte[] {0x03, 0x00, 0x00, 0x00, 0x61, 0x62, 0x63});
    }

    @Test
    void testGetRequestCustomContentTypes() throws Exception {
        Request request = getRequestWithContentOverrides("json");
        // Content type should include the length of the json part.
        assertThat(request.body().contentType().toString()).isEqualTo("application/x-custom");
        // ACCEPT header should be set to the output content type.
        assertThat(request.header("ACCEPT")).isEqualTo("application/extra-custom");
        String expectedJson = "{\"input\":[1,2,3],\"input2\":\"abc\"}";
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo(expectedJson);
    }

    @NotNull
    private static Request getRequestWithContentOverrides(String inputFormat) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("SAGEMAKER.INPUT_FORMAT", inputFormat);
        modelOptions.put("SAGEMAKER.INPUT_CONTENT_TYPE", "application/x-custom");
        modelOptions.put("SAGEMAKER.OUTPUT_CONTENT_TYPE", "application/extra-custom");
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        Schema inputSchema =
                Schema.newBuilder()
                        .column("input", "ARRAY<INT>")
                        .column("input2", "STRING")
                        .build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        Object[] args = new Object[] {new Integer[] {1, 2, 3}, "abc"};
        return sagemakerProvider.getRequest(args);
    }

    @NotNull
    private static Request getRequestBinaryWithFormat(String inputFormat) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("SAGEMAKER.INPUT_FORMAT", inputFormat);
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        Schema inputSchema =
                Schema.newBuilder().column("input", "ARRAY<INT>").column("input2", "BYTES").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        SagemakerProvider sagemakerProvider = new SagemakerProvider(model);
        Object[] args = new Object[] {new Integer[] {1, 2, 3}, "abc".getBytes()};
        return sagemakerProvider.getRequest(args);
    }

    @NotNull
    private static CatalogModel getCatalogModel() {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("SAGEMAKER.output_format", "tf-serving");
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();

        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    @NotNull
    private static CatalogModel getCatalogModelMultiType() {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("SAGEMAKER.output_format", "tf-serving");
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
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
                        .column("input17", "ROW(field1 INT, field2 BOOLEAN)")
                        .build();
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
                        .column("output15", "ARRAY<DOUBLE>")
                        .column("output17", "ROW(field1 INT, field2 BOOLEAN)")
                        .build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    private static Map<String, String> getCommonModelOptions() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "SAGEMAKER.ENDPOINT",
                "https://runtime.sagemaker.us-west-2.amazonaws.com/endpoints/1234/invocations");
        modelOptions.put("SAGEMAKER.AWS_ACCESS_KEY_ID", "fake-id");
        modelOptions.put("SAGEMAKER.AWS_SECRET_ACCESS_KEY", "fake-secret-key");
        modelOptions.put("PROVIDER", "SAGEMAKER");
        modelOptions.put("TASK", ModelTask.CLASSIFICATION.name());
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        return modelOptions;
    }
}
