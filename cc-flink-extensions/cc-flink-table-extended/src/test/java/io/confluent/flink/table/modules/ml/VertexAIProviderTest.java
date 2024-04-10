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

import com.google.auth.oauth2.GoogleCredentials;
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

/** Tests for VertexAIProvider. */
public class VertexAIProviderTest {

    /** Mock VertexAIProvider so we can mock out the google credentials. */
    private static class MockVertexAIProvider extends VertexAIProvider {
        public MockVertexAIProvider(CatalogModel model) {
            super(model);
        }

        @Override
        public GoogleCredentials getCredentials(String serviceKey) {
            return null;
        }

        @Override
        public String getAccessToken() {
            return "fake-token";
        }
    }

    @Test
    void testBadEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("VERTEXAI.ENDPOINT", "fake-endpoint");
        assertThatThrownBy(() -> new MockVertexAIProvider(model))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to be a valid URL");
    }

    @Test
    void testWrongEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("VERTEXAI.ENDPOINT", "https://fake-endpoint.com/something");
        assertThatThrownBy(() -> new MockVertexAIProvider(model))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testGetRequest() throws Exception {
        CatalogModel model = getCatalogModel();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = vertexAIProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://region-aiplatform.googleapis.com/v1/projects/1234/locations/us-central1/endpoints/1234:predict");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("{\"instances\":[\"input-text-prompt\"]}");
    }

    @Test
    void testGetRequestMultiInput() throws Exception {
        CatalogModel model = getCatalogModelMultiType();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
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
        Request request = vertexAIProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://region-aiplatform.googleapis.com/v1/projects/1234/locations/us-central1/endpoints/1234:predict");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"instances\":[{\"input\":\"input-text-prompt\",\"input2\":1,"
                                + "\"input3\":2.0,\"input4\":3,\"input5\":4,\"input6\":true,"
                                + "\"input8\":5,\"input9\":6.0,\"input10\":\"a\",\"input11\":\"b\","
                                + "\"input12_bytes\":{\"b64\":\"Yw==\"},\"input13_bytes\":{\"b64\":\"ZA==\"},\"input14\":7.1,"
                                + "\"input15\":[\"a\",\"b\"],\"input17\":{\"field1\":12,\"field2\":true}}]}");
    }

    @Test
    void testBadResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                vertexAIProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("No predictions found in ML Predict response");
    }

    @Test
    void testErrorResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        String response = "{\"error\":[\"Model not found or something.\"]}";
        assertThatThrownBy(
                        () ->
                                vertexAIProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Model not found or something");
    }

    @Test
    void testParseResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"predictions\":[\"output-text\"]}";
        Row row = vertexAIProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @Test
    void testParseResponseWithObject() throws Exception {
        CatalogModel model = getCatalogModel();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"predictions\":[{\"output\":\"output-text\"}]}";
        Row row = vertexAIProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @Test
    void testParseResponseExtraOutput() throws Exception {
        CatalogModel model = getCatalogModel();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"predictions\":[{\"output\":\"output-text\",\"score\":0.9}]}";
        Row row = vertexAIProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @Test
    void testParseResponseMissingOutput() throws Exception {
        CatalogModel model = getCatalogModel();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"predictions\":[{\"not-output\":\"output-text\",\"score\":0.9}]}";
        assertThatThrownBy(
                        () ->
                                vertexAIProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Field output not found in remote ML Prediction");
    }

    @Test
    void testParseResponseMultiType() throws Exception {
        CatalogModel model = getCatalogModelMultiType();
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response =
                "{\"predictions\":[{\"output\":\"output-text\",\"output2\":3,\"output3\":5.0,"
                        + "\"output4\":1,\"output5\":2,\"output6\":true,\"output7\":\"2021-01-01\","
                        + "\"output8\":10000000000,\"output9\":5.0,\"output10\":\"a\","
                        + "\"output11\":\"b\",\"output12\":\"Yg==\",\"output13\":\"Yg==\","
                        + "\"output14\":5.5,\"output15\":[1.0,2.0],"
                        + "\"output17\":{\"field1\":1,\"field2\":true}}]}";
        Row row = vertexAIProvider.getContentFromResponse(MlUtils.makeResponse(response));
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
        assertThat(request.body().contentType().toString()).isEqualTo("application/octet-stream");
        // The request should be going to the rawPredict endpoint, since this isn't TF Serving.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://region-aiplatform.googleapis.com/v1/projects/1234/locations/us-central1/endpoints/1234:rawPredict");
        String expectedJson =
                "{\"inputs\":["
                        + "{\"name\":\"input\",\"datatype\":\"INT32\",\"shape\":[3],\"data\":[1,2,3]},"
                        + "{\"name\":\"input2\",\"datatype\":\"BYTES\",\"shape\":[3],"
                        + "\"parameters\":{\"binary_data_size\":7}}]}";
        // Inference-Header-Content-Length should be set to the length of the json part.
        int expectedJsonLength = expectedJson.length();
        assertThat(request.headers().get("Inference-Header-Content-Length"))
                .isEqualTo(String.valueOf(expectedJsonLength));
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

    @NotNull
    private static Request getRequestBinaryWithFormat(String inputFormat) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("VERTEXAI.INPUT_FORMAT", inputFormat);
        Schema inputSchema =
                Schema.newBuilder().column("input", "ARRAY<INT>").column("input2", "BYTES").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        VertexAIProvider vertexAIProvider = new MockVertexAIProvider(model);
        Object[] args = new Object[] {new Integer[] {1, 2, 3}, "abc".getBytes()};
        return vertexAIProvider.getRequest(args);
    }

    @NotNull
    private static CatalogModel getCatalogModel() {
        Map<String, String> modelOptions = getCommonModelOptions();
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();

        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    @NotNull
    private static CatalogModel getCatalogModelMultiType() {
        Map<String, String> modelOptions = getCommonModelOptions();
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
                "VERTEXAI.ENDPOINT",
                "https://region-aiplatform.googleapis.com/v1/projects/1234/locations/us-central1/endpoints/1234");
        modelOptions.put("VERTEXAI.SERVICE_KEY", "fake-service-key");
        modelOptions.put("PROVIDER", "VERTEXAI");
        modelOptions.put("TASK", ModelTask.CLASSIFICATION.name());
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        return modelOptions;
    }
}
