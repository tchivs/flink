/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.providers;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.TestUtils.MockSecretDecypterProvider;
import io.confluent.flink.table.utils.RemoteRuntimeUtils;
import okhttp3.Request;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for AzureMLProvider. */
public class AzureMLProviderTest extends ProviderTestBase {

    @Test
    void testBadEndpoint() {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("AZUREML.ENDPOINT", "fake-endpoint");
        assertThatThrownBy(
                        () ->
                                new AzureMLProvider(
                                        model,
                                        new MockSecretDecypterProvider(model, metrics, clock)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(
                        "For AZUREML endpoint expected to match https://[\\w-]+\\.[\\w-]+\\.inference\\.(ml|ai)\\.azure\\.com/.*, got fake-endpoint");
    }

    @Test
    void testInsecureEndpoint() {
        CatalogModel model = getCatalogModel();
        model.getOptions()
                .put(
                        "AZUREML.ENDPOINT",
                        "http://fake-endpoint.fakeregion.inference.ml.azure.com/score");
        assertThatThrownBy(
                        () ->
                                new AzureMLProvider(
                                        model,
                                        new MockSecretDecypterProvider(model, metrics, clock)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(
                        "For AZUREML endpoint, the protocol should be https, got http://fake-endpoint.fakeregion.inference.ml.azure.com/score");
    }

    @Test
    void testTrickyEndpoint() {
        CatalogModel model = getCatalogModel();
        model.getOptions()
                .put(
                        "AZUREML.ENDPOINT",
                        "https://fake-endpoint-mydomain.com/something.fakeregion.inference.ml.azure.com/score");
        assertThatThrownBy(
                        () ->
                                new AzureMLProvider(
                                        model,
                                        new MockSecretDecypterProvider(model, metrics, clock)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testGetRequest() throws Exception {
        CatalogModel model = getCatalogModel();
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = azureMLProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo("https://fake-endpoint.region.inference.ml.azure.com/score");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"input_data\":{\"index\":[0],\"columns\":[\"input\"],\"data\":[[\"input-text-prompt\"]]}}");
    }

    @Test
    void testGetRequestMultiInput() throws Exception {
        CatalogModel model = getCatalogModelMultiType();
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args =
                new Object[] {
                    "input-text-prompt", // STRING
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
                    Row.of(1, true), // ROW(field1 INT, field2 BOOLEAN)
                };
        Request request = azureMLProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo("https://fake-endpoint.region.inference.ml.azure.com/score");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"input_data\":{\"index\":[0],"
                                + "\"columns\":[\"input\",\"input2\",\"input3\",\"input4\",\"input5\","
                                + "\"input6\",\"input8\",\"input9\",\"input10\",\"input11\","
                                + "\"input12\",\"input13\",\"input15\",\"input16\",\"input17\"],"
                                + "\"data\":[[\"input-text-prompt\",1,2.0,3,4,true,5,6.0,\"a\",\"b\","
                                + "\"Yw==\",\"ZA==\",7.1,[\"a\",\"b\"],{\"field1\":1,\"field2\":true}"
                                + "]]}}");
    }

    @Test
    void testBadResponse() {
        CatalogModel model = getCatalogModel();
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                azureMLProvider.getContentFromResponse(
                                        RemoteRuntimeUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("ML prediction response was not a JSON array");
    }

    @Test
    void testErrorResponse() {
        CatalogModel model = getCatalogModel();
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        String response = "{\"message\":[\"Model not found or something.\"]}";
        assertThatThrownBy(
                        () ->
                                azureMLProvider.getContentFromResponse(
                                        RemoteRuntimeUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("ML prediction response was not a JSON array");
    }

    @Test
    void testParseResponse() {
        CatalogModel model = getCatalogModel();
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "[\"output-text\"]";
        Row row = azureMLProvider.getContentFromResponse(RemoteRuntimeUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @Test
    void testParseResponseMissingOutput() {
        CatalogModel model = getCatalogModel();
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "[]";
        assertThatThrownBy(
                        () ->
                                azureMLProvider.getContentFromResponse(
                                        RemoteRuntimeUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Unexpected number of results from ML Predict. Expected 1 but got 0");
    }

    @Test
    void testParseResponseMultiType() {
        CatalogModel model = getCatalogModelMultiType();
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "[[\"foo\"]]";
        Row row = azureMLProvider.getContentFromResponse(RemoteRuntimeUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        String[] output = (String[]) row.getField(0);
        assertThat(output.length).isEqualTo(1);
        assertThat(output[0]).isEqualTo("foo");
    }

    @Test
    void testTypeComparison() {
        UnresolvedDataType dataType = DataTypes.of("string");
        LogicalType logicalType =
                LogicalTypeParser.parse(
                        dataType.toString().substring(1, dataType.toString().length() - 1));
        assertThat(logicalType).isEqualTo(DataTypes.STRING().getLogicalType());
    }

    @Test
    void testGetRequestBinary() throws Exception {
        Request request = getRequestWithFormat("binary");
        assertThat(request.body().contentType().toString()).isEqualTo("application/octet-stream");
        assertThat(request.headers().get("Accept")).isEqualTo("application/octet-stream");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readByteArray())
                .isEqualTo(
                        new byte[] {
                            0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00
                        });
    }

    @Test
    void testGetRequestCsv() throws Exception {
        Request request = getRequestWithFormat("csv");
        assertThat(request.body().contentType().toString()).isEqualTo("text/csv");
        assertThat(request.headers().get("Accept")).isEqualTo("text/csv");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("1,2,3");
    }

    @Test
    void testGetRequestJson() throws Exception {
        Request request = getRequestWithFormat("json");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        assertThat(request.headers().get("Accept")).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("{\"input\":[1,2,3]}");
    }

    @Test
    void testGetRequestJsonArray() throws Exception {
        Request request = getRequestWithFormat("json-array");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        assertThat(request.headers().get("Accept")).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("[1,2,3]");
    }

    @Test
    void testGetRequestText() throws Exception {
        Request request = getRequestStringWithFormat("text");
        assertThat(request.body().contentType().toString()).isEqualTo("text/plain");
        assertThat(request.headers().get("Accept")).isEqualTo("text/plain");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("text-string");
    }

    @Test
    void testGetRequestTfServing() throws Exception {
        Request request = getRequestWithFormat("tf-serving");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        assertThat(request.headers().get("Accept")).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("{\"instances\":[[1,2,3]]}");
    }

    @Test
    void testGetRequestPandasDataframe() throws Exception {
        Request request = getRequestWithFormat("pandas-dataframe");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        assertThat(request.headers().get("Accept")).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"dataframe_split\":{\"index\":[0],\"columns\":[\"input\"],\"data\":[[[1,2,3]]]}}");
    }

    @Test
    void testGetRequestTriton() throws Exception {
        Request request = getRequestWithFormat("triton");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        assertThat(request.headers().get("Accept"))
                .isEqualTo(
                        "application/json,application/octet-stream,"
                                + "application/vnd.sagemaker-triton.binary+json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"inputs\":[{\"name\":\"input\",\"datatype\":\"INT32\","
                                + "\"shape\":[3],\"data\":[1,2,3]}]}");
    }

    @Test
    void testGetRequestTritonMixedBinary() throws Exception {
        Request request = getRequestBinaryWithFormat("triton");
        assertThat(request.body().contentType().toString()).isEqualTo("application/octet-stream");
        assertThat(request.headers().get("Accept"))
                .isEqualTo(
                        "application/json,application/octet-stream,"
                                + "application/vnd.sagemaker-triton.binary+json");
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

    @Test
    void testGetRequestChat() throws Exception {
        CatalogModel model = getAICatalogModel();
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"text-string"};
        Request request = azureMLProvider.getRequest(args);
        // Request should have defaulted to openai-chat format.
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        assertThat(request.headers().get("Accept")).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo("{\"messages\":[{\"role\":\"user\",\"content\":\"text-string\"}]}");
    }

    @NotNull
    private Request getRequestWithFormat(String inputFormat) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("AZUREML.INPUT_FORMAT", inputFormat);
        Schema inputSchema = Schema.newBuilder().column("input", "ARRAY<INT>").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {new Integer[] {1, 2, 3}};
        return azureMLProvider.getRequest(args);
    }

    @NotNull
    private Request getRequestBinaryWithFormat(String inputFormat) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("AZUREML.INPUT_FORMAT", inputFormat);
        Schema inputSchema =
                Schema.newBuilder().column("input", "ARRAY<INT>").column("input2", "BYTES").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {new Integer[] {1, 2, 3}, "abc".getBytes()};
        return azureMLProvider.getRequest(args);
    }

    @NotNull
    private Request getRequestStringWithFormat(String inputFormat) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("AZUREML.INPUT_FORMAT", inputFormat);
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        AzureMLProvider azureMLProvider =
                new AzureMLProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"text-string"};
        return azureMLProvider.getRequest(args);
    }

    @NotNull
    private static CatalogModel getAICatalogModel() {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put(
                "AZUREML.ENDPOINT", "https://fake-endpoint.region.inference.ai.azure.com/score");
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    @NotNull
    public static CatalogModel getCatalogModel() {
        Map<String, String> modelOptions = getCommonModelOptions();
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    @NotNull
    private static CatalogModel getCatalogModelMultiType() {
        Map<String, String> modelOptions = getCommonModelOptions();

        // Create unresolved schema. DataType should be UnresolvedDataType
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
                        .column("input15", "DECIMAL")
                        .column("input16", "ARRAY<STRING>")
                        .column("input17", "ROW(field1 INT, field2 BOOLEAN)")
                        .build();
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<STRING>").build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    private static Map<String, String> getCommonModelOptions() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "AZUREML.ENDPOINT", "https://fake-endpoint.region.inference.ml.azure.com/score");
        modelOptions.put("AZUREML.API_KEY", "fake-api-key");
        modelOptions.put("PROVIDER", "AZUREML");
        modelOptions.put("TASK", "CLASSIFICATION");
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        return modelOptions;
    }
}
