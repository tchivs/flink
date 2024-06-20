/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.providers;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.utils.ml.ModelOptionsUtils;
import io.confluent.flink.table.utils.secrets.SecretDecrypterProvider;
import io.confluent.flink.table.utils.secrets.SecretDecrypterProviderImpl;

import java.time.Clock;
import java.util.Map;

/** Container for functions selecting the correct provider based on the model options. */
public class ProviderSelector {
    public static MLModelRuntimeProvider pickProvider(
            CatalogModel model,
            Map<String, String> configuration,
            MLFunctionMetrics metrics,
            Clock clock) {
        if (model == null) {
            return null;
        }
        String modelProvider = ModelOptionsUtils.getProvider(model.getOptions());
        if (modelProvider.isEmpty()) {
            throw new FlinkRuntimeException("Model PROVIDER option not specified");
        }

        final SecretDecrypterProvider secretDecrypterProvider =
                new SecretDecrypterProviderImpl(model, configuration, metrics, clock);

        if (modelProvider.equalsIgnoreCase(MLModelSupportedProviders.OPENAI.getProviderName())
                || modelProvider.equalsIgnoreCase(
                        MLModelSupportedProviders.AZUREOPENAI.getProviderName())) {
            // OpenAI through their own API or the Azure OpenAI API,
            // which is just a special case of Open AI.
            return new OpenAIProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.GOOGLEAI.getProviderName())) {
            // Any of the Google AI models through their makersuite or generativeapi endpoints.
            return new GoogleAIProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.VERTEXAI.getProviderName())) {
            // GCP Vertex AI models.
            return new VertexAIProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.SAGEMAKER.getProviderName())) {
            // AWS Sagemaker models.
            return new SagemakerProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.BEDROCK.getProviderName())) {
            // AWS Bedrock models.
            return new BedrockProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.AZUREML.getProviderName())) {
            // Azure ML models.
            return new AzureMLProvider(model, secretDecrypterProvider);
        } else {
            throw new UnsupportedOperationException(
                    "Model provider not supported: " + modelProvider);
        }
    }

    public static void validateSchemas(CatalogModel model) {
        if (model == null) {
            return;
        }
        String modelProvider = ModelOptionsUtils.getProvider(model.getOptions());
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model);
        String inputFormat;
        String outputFormat;

        if (modelProvider.equalsIgnoreCase(MLModelSupportedProviders.OPENAI.getProviderName())
                || modelProvider.equalsIgnoreCase(
                        MLModelSupportedProviders.AZUREOPENAI.getProviderName())) {
            inputFormat = OpenAIProvider.getInputFormat(modelOptionsUtils);
            outputFormat = OpenAIProvider.getOutputFormat(modelOptionsUtils, inputFormat);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.GOOGLEAI.getProviderName())) {
            inputFormat = GoogleAIProvider.getInputFormat(modelOptionsUtils);
            outputFormat = GoogleAIProvider.getOutputFormat(modelOptionsUtils, inputFormat);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.VERTEXAI.getProviderName())) {
            inputFormat = VertexAIProvider.getInputFormat(modelOptionsUtils);
            outputFormat = VertexAIProvider.getOutputFormat(modelOptionsUtils, inputFormat);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.SAGEMAKER.getProviderName())) {
            inputFormat = SagemakerProvider.getInputFormat(modelOptionsUtils);
            outputFormat = SagemakerProvider.getOutputFormat(modelOptionsUtils, inputFormat);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.BEDROCK.getProviderName())) {
            inputFormat = BedrockProvider.getInputFormat(modelOptionsUtils);
            outputFormat = BedrockProvider.getOutputFormat(modelOptionsUtils, inputFormat);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.AZUREML.getProviderName())) {
            inputFormat = AzureMLProvider.getInputFormat(modelOptionsUtils);
            outputFormat = AzureMLProvider.getOutputFormat(modelOptionsUtils, inputFormat);
        } else {
            throw new UnsupportedOperationException(
                    "Model provider not supported: " + modelProvider);
        }

        // Get the input and output formatters to validate the schema.
        // They'll throw an exception if the schema is invalid for them.
        try {
            MLFormatterUtil.getInputFormatter(inputFormat, model);
        } catch (Exception e) {
            throw new ValidationException("Invalid Schema for model input: " + e.getMessage(), e);
        }
        try {
            MLFormatterUtil.getOutputParser(outputFormat, model.getOutputSchema().getColumns());
        } catch (Exception e) {
            throw new ValidationException("Invalid Schema for model output: " + e.getMessage(), e);
        }
    }
}
