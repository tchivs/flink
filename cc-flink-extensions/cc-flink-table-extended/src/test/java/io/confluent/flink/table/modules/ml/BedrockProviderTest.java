/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogModel.ModelTask;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.TestUtils.MockSecretDecypterProvider;
import io.confluent.flink.table.utils.MlUtils;
import okhttp3.Request;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for BedrockProvider. */
public class BedrockProviderTest {
    @Test
    void testBadEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("BEDROCK.ENDPOINT", "fake-endpoint");
        assertThatThrownBy(() -> new BedrockProvider(model, new MockSecretDecypterProvider(model)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to be a valid URL");
    }

    @Test
    void testWrongEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("BEDROCK.ENDPOINT", "https://fake-endpoint.com/wrong");
        assertThatThrownBy(() -> new BedrockProvider(model, new MockSecretDecypterProvider(model)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testGetRequest() throws Exception {
        CatalogModel model = getCatalogModel();
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = bedrockProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo("https://bedrock-runtime.us-west-2.amazonaws.com/model/foo/invoke/");
        assertThat(request.method()).isEqualTo("POST");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("{\"inputText\":\"input-text-prompt\"}");
    }

    @Test
    void testBadResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model));
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                bedrockProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Error parsing ML Predict response");
    }

    @Test
    void testErrorResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model));
        String response = "{\"ErrorCode\":[\"Model not found or something.\"]}";
        assertThatThrownBy(
                        () ->
                                bedrockProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Error parsing ML Predict response");
    }

    @Test
    void testParseResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model));
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"results\":[{\"outputText\":\"output-text\"}]}";
        Row row = bedrockProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @NotNull
    private static Request getRequestWithContentOverrides(String inputFormat) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("BEDROCK.INPUT_FORMAT", inputFormat);
        modelOptions.put("BEDROCK.INPUT_CONTENT_TYPE", "application/x-custom");
        modelOptions.put("BEDROCK.OUTPUT_CONTENT_TYPE", "application/extra-custom");
        Schema inputSchema =
                Schema.newBuilder()
                        .column("input", "ARRAY<INT>")
                        .column("input2", "STRING")
                        .build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        CatalogModel model = CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model));
        Object[] args = new Object[] {new Integer[] {1, 2, 3}, "abc"};
        return bedrockProvider.getRequest(args);
    }

    @NotNull
    private static CatalogModel getCatalogModel() {
        Map<String, String> modelOptions = getCommonModelOptions();
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();

        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    private static Map<String, String> getCommonModelOptions() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "BEDROCK.ENDPOINT",
                "https://bedrock-runtime.us-west-2.amazonaws.com/model/foo/invoke");
        modelOptions.put("BEDROCK.AWS_ACCESS_KEY_ID", "fake-id");
        modelOptions.put("BEDROCK.AWS_SECRET_ACCESS_KEY", "fake-secret-key");
        modelOptions.put("PROVIDER", "BEDROCK");
        modelOptions.put("TASK", ModelTask.CLASSIFICATION.name());
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        return modelOptions;
    }
}
