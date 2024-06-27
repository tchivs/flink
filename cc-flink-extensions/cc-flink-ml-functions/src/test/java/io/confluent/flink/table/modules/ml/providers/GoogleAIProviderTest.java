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

/** Tests for GoogleAIProvider. */
public class GoogleAIProviderTest extends ProviderTestBase {

    @Test
    void testBadEndpoint() {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("GOOGLEAI.ENDPOINT", "fake-endpoint");
        assertThatThrownBy(
                        () ->
                                new GoogleAIProvider(
                                        model,
                                        new MockSecretDecypterProvider(model, metrics, clock)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessage(
                        "For GOOGLEAI endpoint expected to match https://generativelanguage.googleapis.com/.*, got fake-endpoint");
    }

    @Test
    void testWrongEndpoint() {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("GOOGLEAI.ENDPOINT", "https://fake-endpoint.com/wrong");
        assertThatThrownBy(
                        () ->
                                new GoogleAIProvider(
                                        model,
                                        new MockSecretDecypterProvider(model, metrics, clock)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testGetRequest() throws Exception {
        CatalogModel model = getCatalogModel();
        GoogleAIProvider googleAIProvider =
                new GoogleAIProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = googleAIProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro?key=fake-api-key");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"contents\":[{\"role\":\"user\",\"parts\":[{\"text\":\"input-text-prompt\"}]}]}");
    }

    @Test
    void testBadResponse() {
        CatalogModel model = getCatalogModel();
        GoogleAIProvider googleAIProvider =
                new GoogleAIProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                googleAIProvider.getContentFromResponse(
                                        RemoteRuntimeUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Expected object field /candidates/0/content/parts/0/text not found in json response");
    }

    @Test
    void testParseResponse() {
        CatalogModel model = getCatalogModel();
        GoogleAIProvider googleAIProvider =
                new GoogleAIProvider(model, new MockSecretDecypterProvider(model, metrics, clock));
        // Response pull the text from json candidates[0].content.parts[0].text
        String response =
                "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"output-text\"}]}}]}";
        Row row =
                googleAIProvider.getContentFromResponse(RemoteRuntimeUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @NotNull
    private static CatalogModel getCatalogModel() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "GOOGLEAI.ENDPOINT",
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro");
        modelOptions.put("GOOGLEAI.API_KEY", "fake-api-key");
        modelOptions.put("PROVIDER", "GOOGLEAI");
        modelOptions.put("TASK", "CLASSIFICATION");
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }
}
