/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.utils.MlUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Formatter for single prompt input. */
public class SinglePromptInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;
    private final TextGenerationParams params;
    private transient ObjectMapper mapper = new ObjectMapper();
    private final SinglePromptFormatter formatter;
    private final ObjectNode staticNode;

    private interface SinglePromptFormatter {
        ObjectNode createStaticJson(TextGenerationParams params);

        default void linkPrompt(ObjectNode node, String prompt) {
            node.put("prompt", prompt);
        }
    }

    public SinglePromptInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns,
            String modelName,
            TextGenerationParams params) {
        this.inputColumns = inputColumns;
        this.params = params;
        MLFormatterUtil.enforceSingleStringInput(inputColumns, modelName);
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            inConverters[i] =
                    DataSerializer.getSerializer(MlUtils.getLogicalType(inputColumns.get(i)));
        }
        formatter = getFormatter(modelName);
        staticNode = formatter.createStaticJson(params);
    }

    @Override
    public byte[] format(Object[] args) {
        if (args.length != inputColumns.size()) {
            throw new FlinkRuntimeException(
                    "ML Predict argument list didn't match model input columns "
                            + args.length
                            + " != "
                            + inputColumns.size());
        }
        String prompt = inConverters[0].toString(args[0]);
        ObjectNode node = staticNode.deepCopy();
        // linkPrompt takes the top level node, even if the prompt is nested.
        formatter.linkPrompt(node, prompt);

        return node.toString().getBytes(StandardCharsets.UTF_8);
    }

    private SinglePromptFormatter getFormatter(String modelName) {
        switch (modelName) {
            case "AI21 Complete":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        // The input json object has all the params at the top level.
                        ObjectNode node = mapper.createObjectNode();
                        // This prompt placeholder just makes sure that the prompt field is first,
                        // which make things prettier.
                        node.put("prompt", "");
                        params.linkTemperature(node, "temperature");
                        params.linkTopP(node, "topP");
                        params.linkMaxTokens(node, "maxTokens");
                        params.linkStopSequences(node, "stopSequences");
                        // TODO: Support all the penalty parameters that this API has.
                        // Note: This API supports a topKReturn parameter, but it is NOT the same as
                        // the topK parameter in the other APIs, and we don't currently support it.
                        return node;
                    }
                };
            case "Amazon Titan Text":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("inputText", "");
                        if (params.hasChatParams()) {
                            ObjectNode textGenerationConfig = mapper.createObjectNode();
                            node.set("textGenerationConfig", textGenerationConfig);
                            params.linkTemperature(textGenerationConfig, "temperature");
                            params.linkTopP(textGenerationConfig, "topP");
                            params.linkMaxTokens(textGenerationConfig, "maxTokenCount");
                            params.linkStopSequences(textGenerationConfig, "stopSequences");
                        }
                        return node;
                    }

                    @Override
                    public void linkPrompt(ObjectNode node, String prompt) {
                        node.put("inputText", prompt);
                    }
                };

            case "Anthropic Completions":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("prompt", "");
                        params.linkTemperature(node, "temperature");
                        params.linkTopP(node, "top_p");
                        params.linkTopK(node, "top_k");
                        params.linkMaxTokens(node, "max_tokens_to_sample");
                        params.linkStopSequences(node, "stop_sequences");
                        return node;
                    }
                };

            case "Anthropic Messages":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        ObjectNode message = mapper.createObjectNode();
                        ObjectNode textContent = mapper.createObjectNode();

                        textContent.put("type", "text");
                        textContent.put("text", "");

                        message.put("role", "user");
                        message.set("content", mapper.createArrayNode().add(textContent));

                        node.set("messages", mapper.createArrayNode().add(message));
                        params.linkTemperature(node, "temperature");
                        params.linkTopP(node, "top_p");
                        params.linkTopK(node, "top_k");
                        params.linkMaxTokens(node, "max_tokens");
                        params.linkStopSequences(node, "stop_sequences");
                        params.linkSystemPrompt(node, "system");
                        // TODO: Pick the default based on the provider? Vertex is currently
                        // "vertex-2023-10-16"
                        params.linkModelVersion(node, "anthropic_version", "bedrock-2023-05-31");
                        return node;
                    }

                    @Override
                    public void linkPrompt(ObjectNode node, String prompt) {
                        ObjectNode message = (ObjectNode) node.get("messages").get(0);
                        ObjectNode textContent = (ObjectNode) message.get("content").get(0);
                        textContent.put("text", prompt);
                    }
                };

            case "Bedrock Llama":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("prompt", "");
                        params.linkTemperature(node, "temperature");
                        params.linkTopP(node, "top_p");
                        params.linkMaxTokens(node, "max_gen_len");
                        return node;
                    }
                };

            case "Cohere Generate":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("prompt", "");
                        params.linkTemperature(node, "temperature");
                        params.linkTopP(node, "p");
                        params.linkTopK(node, "k");
                        params.linkMaxTokens(node, "max_tokens");
                        params.linkStopSequences(node, "stop_sequences");
                        // We don't support streaming.
                        node.put("stream", false);
                        // We always generate a single response.
                        node.put("num_generations", 1);
                        return node;
                    }
                };

            case "Gemini Generate":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        final ArrayNode contents = node.putArray("contents");
                        final ObjectNode content = contents.addObject();
                        final ArrayNode parts = content.putArray("parts");
                        final ObjectNode part = parts.addObject();
                        part.put("text", "");

                        if (params.hasChatParams()) {
                            ObjectNode textGenerationConfig = mapper.createObjectNode();
                            node.set("generationConfig", textGenerationConfig);
                            params.linkTemperature(textGenerationConfig, "temperature");
                            params.linkTopP(textGenerationConfig, "topP");
                            params.linkTopK(textGenerationConfig, "topK");
                            params.linkMaxTokens(textGenerationConfig, "maxOutputTokens");
                            params.linkStopSequences(textGenerationConfig, "stopSequences");
                        }
                        return node;
                    }

                    @Override
                    public void linkPrompt(ObjectNode node, String prompt) {
                        ObjectNode part =
                                (ObjectNode) node.get("contents").get(0).get("parts").get(0);
                        part.put("text", prompt);
                    }
                };

            case "Mistral Completions":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("prompt", "");
                        params.linkTemperature(node, "temperature");
                        params.linkTopP(node, "top_p");
                        params.linkTopK(node, "top_k");
                        params.linkMaxTokens(node, "max_tokens");
                        params.linkStopSequences(node, "stop");
                        return node;
                    }
                };

            case "OpenAI Chat":
                return new SinglePromptFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        // Model id is required, so default to gpt-3.5-turbo unless specified.
                        params.linkModelVersion(node, "model", "gpt-3.5-turbo");
                        params.linkTemperature(node, "temperature");
                        params.linkTopP(node, "top_p");
                        params.linkMaxTokens(node, "max_tokens");
                        params.linkStopSequences(node, "stop");

                        ArrayNode messages = mapper.createArrayNode();
                        if (params.getSystemPrompt() != null) {
                            final ObjectNode messageSystem = messages.addObject();
                            messageSystem.put("role", "system");
                            params.linkSystemPrompt(messageSystem, "content");
                        }

                        ObjectNode userContent = messages.addObject();
                        userContent.put("role", "user");
                        userContent.put("content", "");
                        node.set("messages", messages);
                        return node;
                    }

                    @Override
                    public void linkPrompt(ObjectNode node, String prompt) {
                        // Replace the last message in the array, which is the user message.
                        ArrayNode messages = (ArrayNode) node.get("messages");
                        ObjectNode userContent = (ObjectNode) messages.get(messages.size() - 1);
                        userContent.put("content", prompt);
                    }
                };

            default:
                throw new IllegalArgumentException("Unknown model name: " + modelName);
        }
    }
}
