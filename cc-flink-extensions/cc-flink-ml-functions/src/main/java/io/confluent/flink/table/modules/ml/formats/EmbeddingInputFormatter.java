/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;
import io.confluent.flink.table.utils.mlutils.MlUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Formatter for embedding input. */
public class EmbeddingInputFormatter implements InputFormatter {
    private final List<Schema.UnresolvedColumn> inputColumns;
    private final DataSerializer.InputSerializer[] inConverters;
    private final TextGenerationParams params;
    private transient ObjectMapper mapper = new ObjectMapper();
    private final EmbeddingInputFormatter.EmbeddingFormatter formatter;
    private final ObjectNode staticNode;
    private final MLModelSupportedProviders provider;

    @Override
    public byte[] format(Object[] args) {
        if (args.length != inputColumns.size()) {
            throw new FlinkRuntimeException(
                    "ML Predict argument list didn't match model input columns "
                            + args.length
                            + " != "
                            + inputColumns.size());
        }

        ObjectNode node = staticNode.deepCopy();
        formatter.linkInput(node, mapper, inConverters[0], args[0]);

        return node.toString().getBytes(StandardCharsets.UTF_8);
    }

    private interface EmbeddingFormatter {
        ObjectNode createStaticJson(TextGenerationParams params);

        default void enforceInputs(List<Schema.UnresolvedColumn> inputColumns, String modelName) {
            // By default, we allow only one string input column. Some formatters
            // may allow string arrays or other types.
            MLFormatterUtil.enforceSingleStringInput(inputColumns, modelName);
        }

        default void linkInput(
                ObjectNode node,
                ObjectMapper mapper,
                DataSerializer.InputSerializer inConverter,
                Object arg) {
            node.put("text", inConverter.convert(mapper, null, arg));
        }
    }

    public EmbeddingInputFormatter(
            List<Schema.UnresolvedColumn> inputColumns,
            String modelName,
            TextGenerationParams params,
            MLModelSupportedProviders provider) {
        this.inputColumns = inputColumns;
        this.params = params;
        this.provider = provider;
        inConverters = new DataSerializer.InputSerializer[inputColumns.size()];
        for (int i = 0; i < inputColumns.size(); i++) {
            inConverters[i] =
                    DataSerializer.getSerializer(MlUtils.getLogicalType(inputColumns.get(i)));
        }
        formatter = getFormatter(modelName);
        formatter.enforceInputs(inputColumns, modelName);
        staticNode = formatter.createStaticJson(params);
    }

    private EmbeddingFormatter getFormatter(String modelName) {
        switch (modelName) {
            case "Amazon Titan Embed":
                return new EmbeddingFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("inputText", "");
                        return node;
                    }

                    @Override
                    public void linkInput(
                            ObjectNode node,
                            ObjectMapper mapper,
                            DataSerializer.InputSerializer inConverter,
                            Object arg) {
                        node.put("inputText", inConverter.convert(mapper, null, arg));
                    }
                };
            case "Cohere Embed":
                return new EmbeddingFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        node.put("texts", mapper.createArrayNode());
                        params.linkDefaultParam(
                                node, "input_type", "search_document", "input_type");
                        return node;
                    }

                    @Override
                    public void enforceInputs(
                            List<Schema.UnresolvedColumn> inputColumns, String modelName) {
                        MLFormatterUtil.enforceSingleStringOrArrayInput(inputColumns, modelName);
                    }

                    @Override
                    public void linkInput(
                            ObjectNode node,
                            ObjectMapper mapper,
                            DataSerializer.InputSerializer inConverter,
                            Object arg) {
                        if (arg instanceof String) {
                            ((ArrayNode) node.get("texts"))
                                    .add(inConverter.convert(mapper, null, arg));
                        } else {
                            // reuse the existing array node
                            inConverter.convert(mapper, node.get("texts"), arg);
                        }
                    }
                };
            case "OpenAI Embed":
                return new EmbeddingFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        ObjectNode node = mapper.createObjectNode();
                        // The OpenAI API allows input to be either a string or an array of strings,
                        // but we always send an array. This is mostly because Azure uses this
                        // same API for other embedding models where it requires an array.
                        // Note that we don't currently allow multiple inputs since we don't support
                        // multiple outputs for this model.
                        node.put("input", mapper.createArrayNode());
                        // Model id is required for the OpenAI provider (but not Azure OpenAI),
                        // so we default to text-embedding-3-small unless specified.
                        String defaultModel = null;
                        if (provider == MLModelSupportedProviders.OPENAI) {
                            defaultModel = "text-embedding-3-small";
                        }
                        params.linkModelVersion(node, "model", defaultModel);
                        return node;
                    }

                    @Override
                    public void linkInput(
                            ObjectNode node,
                            ObjectMapper mapper,
                            DataSerializer.InputSerializer inConverter,
                            Object arg) {
                        if (arg instanceof String) {
                            ((ArrayNode) node.get("input"))
                                    .add(inConverter.convert(mapper, null, arg));
                        } else {
                            // Note: This case is currently disallowed by enforceInputs, since
                            // we can't read the multiple outputs in this case.
                            // When it's supported, we'll reuse the existing array node.
                            inConverter.convert(mapper, node.get("input"), arg);
                        }
                    }
                };
            case "Vertex Embed":
                return new EmbeddingFormatter() {
                    @Override
                    public ObjectNode createStaticJson(TextGenerationParams params) {
                        // Format is similar to TFSERVING, but the content field is always named,
                        // even when it's the only field.
                        ObjectNode node = mapper.createObjectNode();
                        ArrayNode instances = node.putArray("instances");
                        ObjectNode instance = instances.addObject();
                        instance.put("content", "");
                        return node;
                    }

                    @Override
                    public void linkInput(
                            ObjectNode node,
                            ObjectMapper mapper,
                            DataSerializer.InputSerializer inConverter,
                            Object arg) {
                        ObjectNode instance =
                                (ObjectNode) ((ArrayNode) node.get("instances")).get(0);
                        instance.put("content", inConverter.convert(mapper, null, arg));
                    }
                };
            default:
                throw new IllegalArgumentException("Unknown model name: " + modelName);
        }
    }
}
