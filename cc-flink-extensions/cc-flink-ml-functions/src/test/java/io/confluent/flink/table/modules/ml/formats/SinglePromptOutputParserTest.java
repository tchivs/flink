/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;

import io.confluent.flink.table.utils.RemoteRuntimeUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for text/chat foundation model parsers. */
public class SinglePromptOutputParserTest {
    @Test
    void testParseAI21Complete() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "AI21 Complete");
        String response = "{\"completions\":[{\"data\":{\"text\":\"output-text\"}}]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseAmazonTitanText() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Amazon Titan Text");
        String response = "{\"results\":[{\"outputText\":\"output-text\"}]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseAnthropicCompletions() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Anthropic Completions");
        String response = "{\"completion\":\"output-text\"}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseAnthropicMessages() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Anthropic Messages");
        String response = "{\"content\":[{\"text\":\"output-text\"}]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseAzureChat() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser = new SinglePromptOutputParser(outputSchema.getColumns(), "Azure Chat");
        String response = "{\"output\":\"output-text\"}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseBedrockLlama() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Bedrock Llama");
        String response = "{\"generation\":\"output-text\"}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseCohereChat() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Cohere Chat");
        String response = "{\"text\":\"output-text\"}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseCohereGenerate() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Cohere Generate");
        String response = "{\"generations\":[{\"text\":\"output-text\"}]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseGeminiChat() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Gemini Generate");
        String response =
                "{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"output-text\"}]}}]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testMistralCompletions() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Mistral Completions");
        String response = "{\"outputs\":[{\"text\":\"output-text\"}]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testMistralChat() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        OutputParser parser =
                new SinglePromptOutputParser(outputSchema.getColumns(), "Mistral Chat");
        String response = "{\"choices\":[{\"message\":{\"content\":\"output-text\"}}]}";
        assertThat(parser.parse(RemoteRuntimeUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }
}
