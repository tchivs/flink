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

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for OpenAIProvider. */
public class OpenAIProviderTest {
    private final MLModelSupportedProviders provider = MLModelSupportedProviders.OPENAI;

    @Test
    void testBadEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("OPENAI.ENDPOINT", "fake-endpoint");
        assertThatThrownBy(() -> new OpenAIProvider(model, provider))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to be a valid URL");
    }

    @Test
    void testWrongEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("OPENAI.ENDPOINT", "https://fake-endpoint.com/wrong");
        assertThatThrownBy(() -> new OpenAIProvider(model, provider))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testGetRequest() throws Exception {
        CatalogModel model = getCatalogModel();
        OpenAIProvider openAIProvider = new OpenAIProvider(model, provider);
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = openAIProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo("https://api.openai.com/v1/chat/completions");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.header("Authorization")).isEqualTo("Bearer fake-api-key");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"model\":\"gpt-3.5-turbo\",\"messages\":["
                                + "{\"role\":\"user\",\"content\":\"input-text-prompt\"}]}");
    }

    @Test
    void testGetRequestAzure() throws Exception {
        CatalogModel model = getCatalogModelAzure();
        OpenAIProvider openAIProvider =
                new OpenAIProvider(model, MLModelSupportedProviders.AZUREOPENAI);
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = openAIProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo(
                        "https://resource.openai.azure.com/openai/deployments/deploymentname/chat/completions");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.header("api-key")).isEqualTo("fake-api-key");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"model\":\"gpt-3.5-turbo\",\"messages\":["
                                + "{\"role\":\"user\",\"content\":\"input-text-prompt\"}]}");
    }

    @Test
    void testGetRequestSystemPrompt() throws Exception {
        CatalogModel model = getCatalogModelSystemPrompt();
        OpenAIProvider openAIProvider = new OpenAIProvider(model, provider);
        Object[] args = new Object[] {"input-text-prompt"};
        Request request = openAIProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString())
                .isEqualTo("https://api.openai.com/v1/chat/completions");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.header("Authorization")).isEqualTo("Bearer fake-api-key");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"model\":\"gpt-3.5-turbo\",\"temperature\":0.7,"
                                + "\"messages\":[{\"role\":\"system\",\"content\":\"System Prompt!\"},"
                                + "{\"role\":\"user\",\"content\":\"input-text-prompt\"}]}");
    }

    @Test
    void testBadResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        OpenAIProvider openAIProvider = new OpenAIProvider(model, provider);
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () -> openAIProvider.getContentFromResponse(MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Expected object field /choices/0/message/content not found in response");
    }

    @Test
    void testParseResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        OpenAIProvider openAIProvider = new OpenAIProvider(model, provider);
        String response =
                "{\"choices\":[{\"message\":{\"role\":\"user\",\"content\":\"output-text\"}}]}";
        Row row = openAIProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.getField(0).toString()).isEqualTo("output-text");
    }

    @NotNull
    private static CatalogModel getCatalogModelAzure() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put(
                "AZUREOPENAI.ENDPOINT",
                "https://resource.openai.azure.com/openai/deployments/deploymentname/chat/completions");
        modelOptions.put("AZUREOPENAI.API_KEY", "fake-api-key");
        modelOptions.put("PROVIDER", "AZUREOPENAI");
        modelOptions.put("TASK", ModelTask.TEXT_GENERATION.name());
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    @NotNull
    private static CatalogModel getCatalogModel() {
        Map<String, String> modelOptions = getCommonModelOptions();
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    @NotNull
    private static CatalogModel getCatalogModelSystemPrompt() {
        Map<String, String> modelOptions = getCommonModelOptions();
        modelOptions.put("OPENAI.SYSTEM_PROMPT", "System Prompt!");
        modelOptions.put("OPENAI.PARAMS.TEMPERATURE", "0.7");
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }

    private static Map<String, String> getCommonModelOptions() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("PROVIDER", "OPENAI");
        modelOptions.put("OPENAI.ENDPOINT", "https://api.openai.com/v1/chat/completions");
        modelOptions.put("OPENAI.API_KEY", "fake-api-key");
        modelOptions.put("TASK", ModelTask.TEXT_GENERATION.name());
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        return modelOptions;
    }
}
