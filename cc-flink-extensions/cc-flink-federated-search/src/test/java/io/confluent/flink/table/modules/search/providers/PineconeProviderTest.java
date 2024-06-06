/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.providers;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.TestUtils.MockSecretDecypterProvider;
import io.confluent.flink.table.modules.ml.providers.SearchSupportedProviders;
import io.confluent.flink.table.utils.mlutils.MlUtils;
import okhttp3.Request;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for PineconeProvider. */
public class PineconeProviderTest extends ProviderTestBase {
    private final SearchSupportedProviders provider = SearchSupportedProviders.PINECONE;

    @Test
    void testGetRequest() throws Exception {
        CatalogTable table = getCatalogTable();
        PineconeProvider pineconeProvider =
                new PineconeProvider(table, new MockSecretDecypterProvider(table, metrics, clock));
        Object[] args = new Object[] {2, new float[] {1.0f, 2.0f}};
        Request request = pineconeProvider.getRequest(args);
        // Check that the request is created correctly.
        assertThat(request.url().toString()).isEqualTo("https://fake-not-yet-validated/");
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.header("Api-Key")).isEqualTo("fake-api-key");
        assertThat(request.body().contentType().toString()).isEqualTo("application/json");
        Buffer buffer = new Buffer();
        request.body().writeTo(buffer);
        assertThat(buffer.readUtf8())
                .isEqualTo(
                        "{\"includeMetadata\":true,\"includeValues\":true,\"topK\":2,\"vector\":[1.0,2.0]}");
    }

    @Test
    void testBadResponse() {
        CatalogTable table = getCatalogTable();
        PineconeProvider pineconeProvider =
                new PineconeProvider(table, new MockSecretDecypterProvider(table, metrics, clock));
        String response = "{\"choices\":[{\"text\":\"output-text\"}]}";
        assertThatThrownBy(
                        () ->
                                pineconeProvider.getContentFromResponse(
                                        MlUtils.makeResponse(response)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("No 'matches' field found in Pinecone response.");
    }

    @Test
    void testParseResponse() {
        CatalogTable table = getCatalogTable();
        PineconeProvider pineconeProvider =
                new PineconeProvider(table, new MockSecretDecypterProvider(table, metrics, clock));
        String response =
                "{\n"
                        + "  \"matches\":[\n"
                        + "    {\n"
                        + "      \"id\": \"vec3\",\n"
                        + "      \"score\": 0,\n"
                        + "      \"values\": [0.3,0.3,0.3,0.3],\n"
                        + "      \"metadata\": {\"key\": \"value1\", \"content\": \"content1\", \"content2\": 3}\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"id\": \"vec2\",\n"
                        + "      \"score\": 0.0800000429,\n"
                        + "      \"values\": [0.2, 0.2, 0.2, 0.2],\n"
                        + "      \"metadata\": {\"key\": \"value2\", \"content\": \"content2\", \"content2\": 4}\n"
                        + "    },\n"
                        + "    {\n"
                        + "      \"id\": \"vec4\",\n"
                        + "      \"score\": 0.0799999237,\n"
                        + "      \"values\": [0.4, 0.4, 0.4, 0.4],\n"
                        + "      \"metadata\": {\"key\": \"value3\", \"content\": \"content3\", \"content2\": 5}\n"
                        + "    }\n"
                        + "  ],\n"
                        + "  \"namespace\": \"example-namespace\",\n"
                        + "  \"usage\": {\"read_units\": 6}\n"
                        + "}";
        Row row = pineconeProvider.getContentFromResponse(MlUtils.makeResponse(response));
        // Check that the response is parsed correctly.
        assertThat(row.getKind()).isEqualTo(RowKind.INSERT);
        assertThat(row.toString())
                .isEqualTo(
                        "+I[+I[content1, [0.3, 0.3, 0.3, 0.3]], +I[content2, [0.2, 0.2, 0.2, 0.2]], +I[content3, [0.4, 0.4, 0.4, 0.4]]]");
    }

    @NotNull
    private static CatalogTable getCatalogTable() {
        Map<String, String> tableOptions = getCommonTableOptions();
        Schema inputSchema =
                Schema.newBuilder()
                        .column("content", "STRING")
                        .column("embedding", "ARRAY<FLOAT>")
                        .build();
        return CatalogTable.of(inputSchema, "", Collections.emptyList(), tableOptions);
    }

    private static Map<String, String> getCommonTableOptions() {
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("PROVIDER", "PINECONE");
        tableOptions.put("PINECONE.ENDPOINT", "https://fake-not-yet-validated/");
        tableOptions.put("PINECONE.API_KEY", "fake-api-key");
        tableOptions.put("CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY", "plaintext");
        return tableOptions;
    }
}
