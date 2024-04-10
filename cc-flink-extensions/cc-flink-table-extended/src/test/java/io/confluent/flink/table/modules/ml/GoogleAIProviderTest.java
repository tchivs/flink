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

/** Tests for GoogleAIProvider. */
public class GoogleAIProviderTest {

    @Test
    void testBadEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("GOOGLEAI.ENDPOINT", "fake-endpoint");
        assertThatThrownBy(() -> new GoogleAIProvider(model))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to be a valid URL");
    }

    @Test
    void testWrongEndpoint() throws Exception {
        CatalogModel model = getCatalogModel();
        model.getOptions().put("GOOGLEAI.ENDPOINT", "https://fake-endpoint.com/wrong");
        assertThatThrownBy(() -> new GoogleAIProvider(model))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("expected to match");
    }

    @Test
    void testGetRequest() throws Exception {
        CatalogModel model = getCatalogModel();
        GoogleAIProvider googleAIProvider = new GoogleAIProvider(model);
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
                .isEqualTo("{\"contents\":[{\"parts\":[{\"text\":\"input-text-prompt\"}]}]}");
    }

    @Test
    void testBadResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        GoogleAIProvider googleAIProvider = new GoogleAIProvider(model);
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                googleAIProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Expected object field /candidates/0/content/parts/0/text not found in response");
    }

    @Test
    void testParseResponse() throws Exception {
        CatalogModel model = getCatalogModel();
        GoogleAIProvider googleAIProvider = new GoogleAIProvider(model);
        // Response pull the text from json candidates[0].content.parts[0].text
        String response =
                "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"output-text\"}]}}]}";
        Row row = googleAIProvider.getContentFromResponse(MlUtils.makeResponse(response));
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
        modelOptions.put("TASK", ModelTask.CLASSIFICATION.name());
        modelOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        return CatalogModel.of(inputSchema, outputSchema, modelOptions, "");
    }
}
