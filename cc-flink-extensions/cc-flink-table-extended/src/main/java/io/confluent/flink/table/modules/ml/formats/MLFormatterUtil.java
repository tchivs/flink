/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.ml.MLModelSupportedProviders;
import io.confluent.flink.table.utils.mlutils.MlUtils;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;

import java.util.List;
import java.util.Map;

/** Utility class for ML model input and output formatters. */
public class MLFormatterUtil {
    public static InputFormatter getInputFormatter(String format, CatalogModel model) {
        Map<String, String> modelOptions = model.getOptions();
        List<Schema.UnresolvedColumn> inputColumns = model.getInputSchema().getColumns();
        List<Schema.UnresolvedColumn> outputColumns = model.getOutputSchema().getColumns();
        String modelProvider = ModelOptionsUtils.getProvider(model.getOptions());
        // The provider string has already been validated so valueOf should never throw.
        MLModelSupportedProviders provider =
                MLModelSupportedProviders.valueOf(modelProvider.toUpperCase());
        // Split the format into possibly two parts separated by a colon.
        String[] parts = format.split(":", 2);
        String wrapper = parts.length > 1 ? parts[1] : null;
        String upperFormat = parts[0].toUpperCase();
        upperFormat = upperFormat.replaceAll("[-_]", "");
        switch (upperFormat) {
            case "BINARY":
                return new BinaryInputFormatter(inputColumns);
            case "CSV":
                return new CSVInputFormatter(inputColumns);
            case "JSON":
            case "JSONOBJECT":
                if (wrapper != null) {
                    return new JsonObjectInputFormatter(inputColumns, wrapper);
                }
                return new JsonObjectInputFormatter(inputColumns);
            case "JSONARRAY":
                return new JsonArrayInputFormatter(inputColumns);
            case "PANDASDATAFRAME":
            case "PANDASDATAFRAMESPLIT":
                if (wrapper != null) {
                    return new PandasDataframeSplitInputFormatter(inputColumns, wrapper);
                }
                return new PandasDataframeSplitInputFormatter(inputColumns);
            case "TEXT":
            case "TXT":
                return new TextInputFormatter(inputColumns);
            case "TFSERVING":
            case "TENSORFLOWSERVING":
            case "TFSERVINGROW":
            case "KSERVEV1":
                if (wrapper != null) {
                    return new TFServingInputFormatter(inputColumns, wrapper);
                }
                return new TFServingInputFormatter(inputColumns);
            case "TRITON":
            case "KSERVEV2":
                return new TritonInputFormatter(inputColumns, outputColumns);
            case "AZUREMLPANDASDATAFRAME":
            case "AZUREMLPANDASDATAFRAMESPLIT":
                // Alias for "PANDAS_DATAFRAME:input_data"
                return new PandasDataframeSplitInputFormatter(inputColumns, "input_data");
            case "VERTEXAIPYTORCH":
                // Alias for "TF_SERVING:data"
                return new TFServingInputFormatter(inputColumns, "data");
            case "TFSERVINGCOL":
            case "TFSERVINGCOLUMN":
            case "MLFLOWTENSOR":
                // Alias for JSON:inputs
                return new JsonObjectInputFormatter(inputColumns, "inputs");
            case "AZUREMLTENSOR":
                // Alias for JSON:input_data
                return new JsonObjectInputFormatter(inputColumns, "input_data");
            case "AI21COMPLETE":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "AI21 Complete",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "AMAZONTITANTEXT":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Amazon Titan Text",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "AMAZONTITANEMBED":
                return new EmbeddingInputFormatter(
                        inputColumns,
                        "Amazon Titan Embed",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "ANTHROPICCOMPLETIONS":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Anthropic Completions",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "ANTHROPICMESSAGES":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Anthropic Messages",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "AZURECHAT":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Azure Chat",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "BEDROCKLLAMA":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Bedrock Llama",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "COHERECHAT":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Cohere Chat",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "COHEREGENERATE":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Cohere Generate",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "COHEREEMBED":
                return new EmbeddingInputFormatter(
                        inputColumns,
                        "Cohere Embed",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "GEMINICHAT":
            case "GEMINIGENERATE":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Gemini Generate",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "MISTRALCHAT":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Mistral Chat",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "MISTRALCOMPLETIONS":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "Mistral Completions",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "OPENAICHAT":
                return new SinglePromptInputFormatter(
                        inputColumns,
                        "OpenAI Chat",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "AZUREEMBED":
            case "OPENAIEMBED":
                return new EmbeddingInputFormatter(
                        inputColumns,
                        "OpenAI Embed",
                        new TextGenerationParams(modelOptions),
                        provider);
            case "VERTEXEMBED":
                return new EmbeddingInputFormatter(
                        inputColumns,
                        "Vertex Embed",
                        new TextGenerationParams(modelOptions),
                        provider);
            default:
                throw new FlinkRuntimeException("Unsupported ML Model input format: " + format);
        }
    }

    public static void enforceSingleStringInput(
            List<Schema.UnresolvedColumn> inputColumns, String format) {
        // We allow a single input column of type STRING.
        if (inputColumns.size() != 1
                || (MlUtils.getLogicalType(inputColumns.get(0)).getTypeRoot()
                                != LogicalTypeRoot.VARCHAR
                        && MlUtils.getLogicalType(inputColumns.get(0)).getTypeRoot()
                                != LogicalTypeRoot.CHAR)) {
            throw new FlinkRuntimeException(
                    format
                            + " input format requires a single input column of type STRING, CHAR, or VARCHAR.");
        }
    }

    public static void enforceSingleStringOrArrayInput(
            List<Schema.UnresolvedColumn> inputColumns, String format) {
        // We allow a single input column of type STRING or ARRAY<STRING>.
        if (inputColumns.size() != 1) {
            throw new FlinkRuntimeException(
                    format
                            + " input format requires a single input column of type STRING, CHAR, or ARRAY<STRING>.");
        }
        LogicalType type = MlUtils.getLogicalType(inputColumns.get(0));
        if (type.getTypeRoot() != LogicalTypeRoot.VARCHAR
                && type.getTypeRoot() != LogicalTypeRoot.CHAR
                && type.getTypeRoot() != LogicalTypeRoot.ARRAY) {
            throw new FlinkRuntimeException(
                    format
                            + " input format requires a single input column of type STRING, CHAR, or ARRAY<STRING>.");
        }
        if (type.getTypeRoot() == LogicalTypeRoot.ARRAY
                && type.getChildren().get(0).getTypeRoot() != LogicalTypeRoot.VARCHAR
                && type.getChildren().get(0).getTypeRoot() != LogicalTypeRoot.CHAR) {
            throw new FlinkRuntimeException(
                    format
                            + " input format requires a single input column of type STRING, CHAR, or ARRAY<STRING>.");
        }
    }

    public static void enforceSingleStringOutput(
            List<Schema.UnresolvedColumn> outputColumns, String format) {
        // We allow a single output column of type STRING/VARCHAR/CHAR.
        if (outputColumns.size() != 1
                || (MlUtils.getLogicalType(outputColumns.get(0)).getTypeRoot()
                                != org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR
                        && MlUtils.getLogicalType(outputColumns.get(0)).getTypeRoot()
                                != org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR)) {
            throw new FlinkRuntimeException(
                    format
                            + " output format requires a single output column of type STRING, CHAR, or VARCHAR.");
        }
    }

    public static OutputParser getOutputParser(
            String outputFormat, List<Schema.UnresolvedColumn> outputColumns) {
        String[] parts = outputFormat.split(":", 2);
        String wrapper = parts.length > 1 ? parts[1] : null;
        String upperFormat = parts[0].toUpperCase();
        upperFormat = upperFormat.replaceAll("[-_]", "");
        switch (upperFormat) {
            case "BINARY":
                return new BinaryOutputParser(outputColumns);
            case "CSV":
                return new CSVOutputParser(outputColumns);
            case "JSON":
            case "JSONOBJECT":
                if (wrapper != null) {
                    return new JsonObjectOutputParser(outputColumns, wrapper);
                }
                return new JsonObjectOutputParser(outputColumns);
            case "JSONARRAY":
                if (wrapper != null) {
                    return new JsonArrayOutputParser(outputColumns, wrapper);
                }
                return new JsonArrayOutputParser(outputColumns);
            case "TEXT":
            case "TXT":
                return new TextOutputParser(outputColumns);
            case "TFSERVING":
            case "TENSORFLOWSERVING":
            case "KSERVEV1":
            case "JSONARRAYOFOBJECTS":
                if (wrapper != null) {
                    return new TFServingOutputParser(outputColumns, wrapper);
                }
                return new TFServingOutputParser(outputColumns);
            case "TRITON":
            case "KSERVEV2":
                return new TritonOutputParser(outputColumns);
            case "AI21COMPLETE":
                return new SinglePromptOutputParser(outputColumns, "AI21 Complete");
            case "AMAZONTITANTEXT":
                return new SinglePromptOutputParser(outputColumns, "Amazon Titan Text");
            case "AMAZONTITANEMBED":
                return new EmbeddingOutputParser(outputColumns, "Amazon Titan Embed");
            case "ANTHROPICCOMPLETIONS":
                return new SinglePromptOutputParser(outputColumns, "Anthropic Completions");
            case "ANTHROPICMESSAGES":
                return new SinglePromptOutputParser(outputColumns, "Anthropic Messages");
            case "AZURECHAT":
                return new SinglePromptOutputParser(outputColumns, "Azure Chat");
            case "BEDROCKLLAMA":
                return new SinglePromptOutputParser(outputColumns, "Bedrock Llama");
            case "COHERECHAT":
                return new SinglePromptOutputParser(outputColumns, "Cohere Chat");
            case "COHEREGENERATE":
                return new SinglePromptOutputParser(outputColumns, "Cohere Generate");
            case "COHEREEMBED":
                return new EmbeddingOutputParser(outputColumns, "Cohere Embed");
            case "GEMINICHAT":
            case "GEMINIGENERATE":
                return new SinglePromptOutputParser(outputColumns, "Gemini Generate");
            case "MISTRALCHAT":
                return new SinglePromptOutputParser(outputColumns, "Mistral Chat");
            case "MISTRALCOMPLETIONS":
                return new SinglePromptOutputParser(outputColumns, "Mistral Completions");
            case "OPENAICHAT":
                return new SinglePromptOutputParser(outputColumns, "OpenAI Chat");
            case "AZUREEMBED":
            case "OPENAIEMBED":
                return new EmbeddingOutputParser(outputColumns, "OpenAI Embed");
            case "VERTEXEMBED":
                return new EmbeddingOutputParser(outputColumns, "Vertex Embed");
            default:
                throw new FlinkRuntimeException(
                        "Unsupported ML Model output format: " + outputFormat);
        }
    }

    public static String defaultOutputFormat(String inputFormat) {
        String[] parts = inputFormat.split(":", 2);
        String wrapper = parts.length > 1 ? parts[1] : null;
        String upperFormat = parts[0].toUpperCase();
        upperFormat = upperFormat.replaceAll("[-_]", "");
        switch (upperFormat) {
            case "PANDASDATAFRAME":
            case "PANDASDATAFRAMESPLIT":
            case "AZUREMLPANDASDATAFRAME":
            case "AZUREMLPANDASDATAFRAMESPLIT":
                // These tend to be SKLearn models.
                return "JSON_ARRAY";
            case "VERTEXAIPYTORCH":
                return "TF_SERVING";
            case "TFSERVINGCOL":
            case "TFSERVINGCOLUMN":
            case "MLFLOWTENSOR":
            case "AZUREMLTENSOR":
                // JSON:outputs is the TF Serving Column output format.
                return "JSON:outputs";
            default:
                // Everything else defaults to itself.
                return inputFormat;
        }
    }
}
