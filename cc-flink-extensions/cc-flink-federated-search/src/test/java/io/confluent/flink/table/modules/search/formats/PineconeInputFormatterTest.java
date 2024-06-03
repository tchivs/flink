/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.ml.formats.TextGenerationParams;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for pinecone input formatter. */
public class PineconeInputFormatterTest {
    @Test
    void testGetRequest() throws Exception {
        Schema inputSchema =
                Schema.newBuilder().column("k", "INT").column("vectors", "ARRAY<FLOAT>").build();
        PineconeInputFormatter formatter =
                new PineconeInputFormatter(inputSchema.getColumns(), getEmptyParams());
        Object[] args = new Object[] {3, new float[] {0.1f, 0.2f, 0.3f, 0.4f}};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"includeMetadata\":true,\"includeValues\":true,\"topK\":3,\"vector\":[0.1,0.2,0.3,0.4]}");
    }

    @Test
    void testGetRequestWithFilter() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("top_k", "BIGINT")
                        .column("embeds", "ARRAY<DOUBLE>")
                        .build();
        PineconeInputFormatter formatter =
                new PineconeInputFormatter(inputSchema.getColumns(), getFilterParams());
        Object[] args = new Object[] {3L, new double[] {0.1, 0.2, 0.3, 0.4}};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"filter\":{\"genre\":{\"$eq\":\"documentary\"}},\"includeMetadata\":true,\"includeValues\":true,\"topK\":3,\"vector\":[0.1,0.2,0.3,0.4]}");
    }

    @Test
    void testGetRequestInvalidEmbedding() throws Exception {
        Schema inputSchema =
                Schema.newBuilder().column("k", "BIGINT").column("vectors", "DOUBLE").build();
        assertThatThrownBy(
                        () ->
                                new PineconeInputFormatter(
                                        inputSchema.getColumns(), getEmptyParams()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Pinecone vector input must be an array, got DOUBLE");
    }

    @Test
    void testGetRequestInvalidTopK() throws Exception {
        Schema inputSchema =
                Schema.newBuilder()
                        .column("k", "STRING")
                        .column("vectors", "ARRAY<DOUBLE>")
                        .build();
        assertThatThrownBy(
                        () ->
                                new PineconeInputFormatter(
                                        inputSchema.getColumns(), getEmptyParams()))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Pinecone topK input must be an integer, got STRING");
    }

    private static TextGenerationParams getEmptyParams() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("PROVIDER", "PINECONE");
        return new TextGenerationParams(modelOptions);
    }

    private static TextGenerationParams getFilterParams() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("PROVIDER", "PINECONE");
        modelOptions.put("PINECONE.PARAMS.filter.genre.$eq", "documentary");
        modelOptions.put("PINECONE.PARAMS.includevalues", "false");
        return new TextGenerationParams(modelOptions);
    }
}
