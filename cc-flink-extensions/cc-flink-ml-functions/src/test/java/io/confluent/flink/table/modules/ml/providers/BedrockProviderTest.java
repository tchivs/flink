/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.providers;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.TestUtils.MockSecretDecypterProvider;
import io.confluent.flink.table.utils.RemoteRuntimeUtils;
import okhttp3.Request;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for BedrockProvider. */
public class BedrockProviderTest extends ProviderTestBase {
    @Test
    void testBadEndpoint() {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("BEDROCK.ENDPOINT", "fake-endpoint");
        assertThatThrownBy(
                        () ->
                                new BedrockProvider(
                                        model,
                                        new MockSecretDecypterProvider(model, metrics, clock)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(
                        "For BEDROCK endpoint expected to match https://bedrock-runtime(-fips)?\\.[\\w-]+\\.amazonaws\\.com/model/.+/invoke/?, got fake-endpoint");
    }

    @Test
    void testWrongEndpoint() {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("BEDROCK.ENDPOINT", "https://fake-endpoint.com/wrong");
        assertThatThrownBy(
                        () ->
                                new BedrockProvider(
                                        model,
                                        new MockSecretDecypterProvider(model, metrics, clock)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testGetRequest() throws Exception {
        CatalogModel model = getCatalogModel();
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
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
    void testBadResponse() {
        CatalogModel model = getCatalogModel();
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                bedrockProvider.getContentFromResponse(
                                        RemoteRuntimeUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Error parsing ML Predict response");
    }

    @Test
    void testErrorResponse() {
        CatalogModel model = getCatalogModel();
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        String response = "{\"ErrorCode\":[\"Model not found or something.\"]}";
        assertThatThrownBy(
                        () ->
                                bedrockProvider.getContentFromResponse(
                                        RemoteRuntimeUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Error parsing ML Predict response");
    }

    @Test
    void testGetRequestEmbed() throws Exception {
        CatalogModel model = getEmbedModel("amazon.titan-embed");
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = bedrockProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://bedrock-runtime.us-west-2.amazonaws.com/model/amazon.titan-embed/invoke/");
        assertThat(request.method()).isEqualTo("POST");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8()).isEqualTo("{\"inputText\":\"input-text-prompt\"}");
    }

    @Test
    void testGetRequestAI21() throws Exception {
        CatalogModel model = getEndpointModel("ai21.whatever");
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = bedrockProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://bedrock-runtime.us-west-2.amazonaws.com/model/ai21.whatever/invoke/");
        assertThat(request.method()).isEqualTo("POST");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        // Request should default to the AI21-COMPLETE format.
        assertThat(buffer.readUtf8()).isEqualTo("{\"prompt\":\"input-text-prompt\"}");
    }

    @Test
    void testGetRequestAnthropic() throws Exception {
        CatalogModel model = getEndpointModel("anthropic.whatever");
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = bedrockProvider.getRequest(args);
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        // Request should default to the ANTHROPIC-MESSAGES format.
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\","
                                + "\"text\":\"input-text-prompt\"}]}],\"anthropic_version\":\"bedrock-2023-05-31\"}");
    }

    @Test
    void testGetRequestCohere() throws Exception {
        CatalogModel model = getEndpointModel("cohere.whatever");
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = bedrockProvider.getRequest(args);
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        // Request should default to the COHERE-GENERATE format.
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"prompt\":\"input-text-prompt\",\"stream\":false,\"num_generations\":1}");

        model = getEmbedModel("cohere.embed");
        bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        request = bedrockProvider.getRequest(args);
        buffer = new Buffer();
        request.body().writeTo(buffer);
        // Request should default to the COHERE-EMBED format.
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"texts\":[\"input-text-prompt\"],\"input_type\":\"search_document\"}");

        model = getEndpointModel("cohere.command-r");
        bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        request = bedrockProvider.getRequest(args);
        buffer = new Buffer();
        request.body().writeTo(buffer);
        // Request should default to the COHERE-CHAT format.
        assertThat(buffer.readUtf8()).isEqualTo("{\"message\":\"input-text-prompt\"}");
    }

    @Test
    void testGetRequestMeta() throws Exception {
        CatalogModel model = getEndpointModel("meta.whatever");
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = bedrockProvider.getRequest(args);
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        // Request should default to the BEDROCK-LLAMA format.
        assertThat(buffer.readUtf8()).isEqualTo("{\"prompt\":\"input-text-prompt\"}");
    }

    @Test
    void testGetRequestMistral() throws Exception {
        CatalogModel model = getEndpointModel("mistral.whatever");
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = bedrockProvider.getRequest(args);
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        // Request should default to the MISTRAL-COMPLETIONS format.
        assertThat(buffer.readUtf8()).isEqualTo("{\"prompt\":\"input-text-prompt\"}");
    }

    @Test
    void testParseResponse() {
        CatalogModel model = getCatalogModel();
        BedrockProvider bedrockProvider =
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        // Response pull the text from json candidates[0].content.parts[0].text
        String response = "{\"results\":[{\"outputText\":\"output-text\"}]}";
        Row row = bedrockProvider.getContentFromResponse(RemoteRuntimeUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @NotNull
    private Request getRequestWithContentOverrides(String inputFormat) {
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
                new BedrockProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {new Integer[] {1, 2, 3}, "abc"};
        return bedrockProvider.getRequest(args);
    }

    @NotNull
    private static CatalogModel getEndpointModel(String endpointModel) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put(
                "BEDROCK.ENDPOINT",
                "https://bedrock-runtime.us-west-2.amazonaws.com/model/"
                        + endpointModel
                        + "/invoke");
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();

        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    @NotNull
    private static CatalogModel getEmbedModel(String endpointModel) {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put(
                "BEDROCK.ENDPOINT",
                "https://bedrock-runtime.us-west-2.amazonaws.com/model/"
                        + endpointModel
                        + "/invoke");
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<FLOAT>").build();

        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
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
        modelOptions.put("TASK", "CLASSIFICATION");
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        return modelOptions;
    }
}
