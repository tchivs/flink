/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.providers;

import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.modules.ml.options.AzureMLRemoteModelOptions;
import io.confluent.flink.table.modules.ml.secrets.SecretDecrypterProvider;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.Map;
import java.util.Objects;

/** Implements Model Runtime for Azure ML API. */
public class AzureMLProvider implements MLModelRuntimeProvider {
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private final CatalogModel model;
    private final transient ObjectMapper mapper = new ObjectMapper();
    private final String apiKey;
    private final String endpoint;
    private final String deploymentName;
    private final InputFormatter inputFormatter;
    private final OutputParser outputParser;
    private final MediaType contentType;
    private final String acceptedContentType;
    private final SecretDecrypterProvider secretDecrypterProvider;
    private final String metricsName;

    public AzureMLProvider(CatalogModel model, SecretDecrypterProvider secretDecrypterProvider) {
        this.model = model;
        this.secretDecrypterProvider =
                Objects.requireNonNull(secretDecrypterProvider, "SecretDecrypterProvider");
        MLModelSupportedProviders supportedProvider = MLModelSupportedProviders.AZUREML;
        String namespace = supportedProvider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model, namespace);
        this.endpoint = modelOptionsUtils.getProviderOption(MLModelCommonConstants.ENDPOINT);
        supportedProvider.validateEndpoint(endpoint, true);

        // Azure ML can take either an API Key or an expiring token, but we only support API Key.
        this.apiKey =
                secretDecrypterProvider
                        .getMeteredDecrypter(modelOptionsUtils.getEncryptStrategy())
                        .decryptFromKey(AzureMLRemoteModelOptions.API_KEY.key());

        // The Azure ML Deployment Name is optional, but allows the user to distinguish between
        // two models that are deployed to the same endpoint.
        this.deploymentName = modelOptionsUtils.getProviderOptionOrDefault("deployment_name", "");
        if (endpoint.contains("inference.ai.azure.com")) {
            metricsName = MLFunctionMetrics.AZURE_ML_AI;
        } else {
            metricsName = namespace;
        }
        String inputFormat = getInputFormat(modelOptionsUtils);
        String outputFormat = getOutputFormat(modelOptionsUtils, inputFormat);
        inputFormatter = MLFormatterUtil.getInputFormatter(inputFormat, model);
        String inputContentType =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "input_content_type", inputFormatter.contentType());
        contentType = MediaType.parse(inputContentType);
        outputParser =
                MLFormatterUtil.getOutputParser(outputFormat, model.getOutputSchema().getColumns());
        acceptedContentType =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "output_content_type", outputParser.acceptedContentTypes());
    }

    public static String getInputFormat(ModelOptionsUtils modelOptionsUtils) {
        // By default, we use the Pandas DataFrame Split format for input, with Azure ML's slight
        // variation of the top level node name.
        String defaultInputFormat = "azureml-pandas-dataframe";
        // If the endpoint looks like an Azure AI endpoint, we default to the openai chat format.
        // Almost all the Azure AI endpoints seem to use that format.
        String endpoint =
                modelOptionsUtils.getProviderOptionOrDefault(MLModelCommonConstants.ENDPOINT, "");
        if (endpoint.contains("inference.ai.azure.com")) {
            defaultInputFormat = "openai-chat";
        }
        return modelOptionsUtils.getProviderOptionOrDefault("input_format", defaultInputFormat);
    }

    public static String getOutputFormat(ModelOptionsUtils modelOptionsUtils, String inputFormat) {
        return modelOptionsUtils.getProviderOptionOrDefault(
                "output_format", MLFormatterUtil.defaultOutputFormat(inputFormat));
    }

    @Override
    public RequestBody getRequestBody(Object[] args) {
        return RequestBody.create(contentType, inputFormatter.format(args));
    }

    @Override
    public Request getRequest(Object[] args) {
        Request.Builder builder =
                new Request.Builder()
                        .url(endpoint)
                        .post(getRequestBody(args))
                        .header(AUTHORIZATION_HEADER, "Bearer " + apiKey);
        if (!deploymentName.isEmpty()) {
            builder.header("azureml-model-deployment", deploymentName);
        }
        for (Map.Entry<String, String> header : inputFormatter.headers()) {
            builder.header(header.getKey(), header.getValue());
        }
        builder.header("Accept", acceptedContentType);
        return builder.build();
    }

    @Override
    public String maskSecrets(String message) {
        return message.replaceAll(apiKey, "*****");
    }

    @Override
    public String getMetricsName() {
        return metricsName;
    }

    @Override
    public Row getContentFromResponse(Response response) {
        return outputParser.parse(response);
    }
}
