/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.utils.RemoteRuntimeUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Formatter for OpenAI chat inputs. */
public class OpenAIChatInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;
    private final TextGenerationParams params;
    private transient ObjectMapper mapper = new ObjectMapper();

    public OpenAIChatInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns, TextGenerationParams params) {
        this.inputColumns = inputColumns;
        this.params = params;
        // We allow a single input column of type STRING.
        MLFormatterUtil.enforceSingleStringInput(inputColumns, "OpenAI Chat");
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            inConverters[i] =
                    DataSerializer.getSerializer(
                            RemoteRuntimeUtils.getLogicalType(inputColumns.get(i)));
        }
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
        userContent.put("content", prompt);

        node.set("messages", messages);
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }
}
