/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;

import io.confluent.flink.table.modules.ml.MLModelSupportedProviders;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for embedding input formatter. */
public class EmbeddingInputFormatterTest {
    @Test
    void testAmazonTitanEmbedInputFormatter() {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new EmbeddingInputFormatter(
                        inputSchema.getColumns(),
                        "Amazon Titan Embed",
                        getTextGenerationParams(),
                        MLModelSupportedProviders.BEDROCK);
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo("{\"inputText\":\"input-text-prompt\"}");
    }

    @Test
    void testCohereEmbedInputFormatter() {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new EmbeddingInputFormatter(
                        inputSchema.getColumns(),
                        "Cohere Embed",
                        getTextGenerationParams(),
                        MLModelSupportedProviders.BEDROCK);
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"texts\":[\"input-text-prompt\"],\"input_type\":\"special_clustering\"}");
    }

    @Test
    void testOpenAIEmbedInputFormatter() {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new EmbeddingInputFormatter(
                        inputSchema.getColumns(),
                        "OpenAI Embed",
                        getTextGenerationParams(),
                        MLModelSupportedProviders.OPENAI);
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo("{\"input\":[\"input-text-prompt\"],\"model\":\"model-ver-1\"}");
    }

    @Test
    void testVertexEmbedInputFormatter() {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new EmbeddingInputFormatter(
                        inputSchema.getColumns(),
                        "Vertex Embed",
                        getTextGenerationParams(),
                        MLModelSupportedProviders.BEDROCK);
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo("{\"instances\":[{\"content\":\"input-text-prompt\"}]}");
    }

    private static TextGenerationParams getTextGenerationParams() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("PROVIDER", "BEDROCK");
        modelOptions.put("BEDROCK.MODEL_VERSION", "model-ver-1");
        modelOptions.put("BEDROCK.PARAMS.input_type", "special_clustering");
        return new TextGenerationParams(modelOptions);
    }
}
