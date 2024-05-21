/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;

import java.util.List;

/** Parser for single prompt outputs. */
public class SinglePromptOutputParser extends JsonObjectOutputParser {
    public SinglePromptOutputParser(List<Schema.UnresolvedColumn> outputColumns, String modelName) {
        super(outputColumns, getJsonPathFromModelName(modelName));
        MLFormatterUtil.enforceSingleStringOutput(outputColumns, modelName);
    }

    private static String getJsonPathFromModelName(String modelName) {
        switch (modelName) {
            case "AI21 Complete":
                return "/completions/0/data/text";
            case "Amazon Titan Text":
                return "/results/0/outputText";
            case "Anthropic Completions":
                return "/completion";
            case "Anthropic Messages":
                return "/content/0/text";
            case "Azure Chat":
                return "/output";
            case "Bedrock Llama":
                return "/generation";
            case "Cohere Chat":
                return "/text";
            case "Cohere Generate":
                return "/generations/0/text";
            case "Gemini Generate":
                return "/candidates/0/content/parts/0/text";
            case "Mistral Completions":
                return "/outputs/0/text";
            case "Mistral Chat":
            case "OpenAI Chat":
                return "/choices/0/message/content";
            default:
                throw new IllegalArgumentException(
                        "Unknown model name for single prompt output: " + modelName);
        }
    }
}
