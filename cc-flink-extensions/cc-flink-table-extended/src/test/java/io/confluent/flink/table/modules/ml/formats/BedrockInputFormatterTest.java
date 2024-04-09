/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unittests for all the Bedrock Foundation model formatters. */
public class BedrockInputFormatterTest {
    @Test
    void testAI21CompleteInputFormatter() throws Exception {

        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new SinglePromptInputFormatter(
                        inputSchema.getColumns(), "AI21 Complete", getTextGenerationParams());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"prompt\":\"input-text-prompt\",\"temperature\":0.5,"
                                + "\"topP\":0.9,\"maxTokens\":100,\"stopSequences\":[\"stop1\",\"stop2\"]}");
    }

    @Test
    void testAmazonTitanTextInputFormatter() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new SinglePromptInputFormatter(
                        inputSchema.getColumns(), "Amazon Titan Text", getTextGenerationParams());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"inputText\":\"input-text-prompt\",\"textGenerationConfig\":{"
                                + "\"temperature\":0.5,\"topP\":0.9,\"maxTokenCount\":100,"
                                + "\"stopSequences\":[\"stop1\",\"stop2\"]}}");
    }

    @Test
    void testAnthropicCompletionsInputFormatter() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new SinglePromptInputFormatter(
                        inputSchema.getColumns(),
                        "Anthropic Completions",
                        getTextGenerationParams());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"prompt\":\"input-text-prompt\",\"temperature\":0.5,\"top_p\":0.9,"
                                + "\"top_k\":10.0,\"max_tokens_to_sample\":100,\"stop_sequences\":[\"stop1\",\"stop2\"]}");
    }

    @Test
    void testAnthropicMessagesInputFormatter() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new SinglePromptInputFormatter(
                        inputSchema.getColumns(), "Anthropic Messages", getTextGenerationParams());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"messages\":[{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"input-text-prompt\"}]}],\"temperature\":0.5,\"top_p\":0.9,\"top_k\":10.0,\"max_tokens\":100,\"stop_sequences\":[\"stop1\",\"stop2\"],\"system\":\"System Prompt!\",\"anthropic_version\":\"bedrock-2023-05-31\"}");
    }

    @Test
    void testBedrockLlamaInputFormatter() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new SinglePromptInputFormatter(
                        inputSchema.getColumns(), "Bedrock Llama", getTextGenerationParams());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"prompt\":\"input-text-prompt\",\"temperature\":0.5,"
                                + "\"top_p\":0.9,\"max_gen_len\":100}");
    }

    @Test
    void testCohereGenerateInputFormatter() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new SinglePromptInputFormatter(
                        inputSchema.getColumns(), "Cohere Generate", getTextGenerationParams());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"prompt\":\"input-text-prompt\",\"temperature\":0.5,\"p\":0.9,"
                                + "\"k\":10.0,\"max_tokens\":100,\"stop_sequences\":[\"stop1\",\"stop2\"],"
                                + "\"stream\":false,\"num_generations\":1}");
    }

    @Test
    void testMistralCompletionsInputFormatter() throws Exception {
        Schema inputSchema = Schema.newBuilder().column("input", "STRING").build();
        InputFormatter formatter =
                new SinglePromptInputFormatter(
                        inputSchema.getColumns(), "Mistral Completions", getTextGenerationParams());
        Object[] args = new Object[] {"input-text-prompt"};
        assertThat(new String(formatter.format(args)))
                .isEqualTo(
                        "{\"prompt\":\"input-text-prompt\",\"temperature\":0.5,\"top_p\":0.9,"
                                + "\"top_k\":10.0,\"max_tokens\":100,\"stop\":[\"stop1\",\"stop2\"]}");
    }

    private static TextGenerationParams getTextGenerationParams() {
        Map<String, String> modelOptions = new HashMap<>();
        modelOptions.put("PROVIDER", "BEDROCK");
        modelOptions.put("BEDROCK.PARAMS.TEMPERATURE", "0.5");
        modelOptions.put("BEDROCK.PARAMS.MAX_K", "10");
        modelOptions.put("BEDROCK.PARAMS.MAX_P", "0.9");
        modelOptions.put("BEDROCK.PARAMS.MAX_TOKENS", "100");
        modelOptions.put("BEDROCK.PARAMS.STOP_SEQUENCES", "stop1,stop2");
        modelOptions.put("BEDROCK.SYSTEM_PROMPT", "System Prompt!");
        return new TextGenerationParams(modelOptions);
    }
}
