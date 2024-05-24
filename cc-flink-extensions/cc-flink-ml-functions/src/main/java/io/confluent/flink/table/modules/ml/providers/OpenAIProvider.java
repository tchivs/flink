/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.providers;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.modules.ml.options.AzureOpenAIRemoteModelOptions;
import io.confluent.flink.table.modules.ml.options.OpenAIRemoteModelOptions;
import io.confluent.flink.table.modules.ml.secrets.SecretDecrypterProvider;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.Map;
import java.util.Objects;

/** Implements Model Runtime for OpenAI. */
public class OpenAIProvider implements MLModelRuntimeProvider {
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String AZURE_AUTH_HEADER = "api-key";
    private final InputFormatter inputFormatter;
    private final OutputParser outputParser;
    private final MediaType contentType;
    private final String acceptedContentType;
    private final String apiKey;
    private final String endpoint;
    private final MLModelSupportedProviders provider;
    private final SecretDecrypterProvider secretDecrypterProvider;
    private final String metricsName;

    public OpenAIProvider(CatalogModel model, SecretDecrypterProvider secretDecrypterProvider) {
        this.provider =
                MLModelSupportedProviders.fromString(
                        ModelOptionsUtils.getProvider(model.getOptions()));
        this.secretDecrypterProvider =
                Objects.requireNonNull(secretDecrypterProvider, "secreteDecrypterProvider");
        MLModelCommonConstants.ModelKind modelKind;
        try {
            modelKind = ModelOptionsUtils.getModelKind(model.getOptions());
        } catch (ValidationException e) {
            throw new FlinkRuntimeException(
                    "Failed to get model kind from model options: " + e.getMessage(), e);
        }

        if (!modelKind.equals(MLModelCommonConstants.ModelKind.REMOTE)) {
            throw new FlinkRuntimeException(
                    "For OpenAI, ML Predict expected a remote model, got " + modelKind.name());
        }
        final String namespace = provider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model, namespace);

        metricsName = provider.getProviderName();
        if (provider == MLModelSupportedProviders.OPENAI) {
            this.endpoint =
                    modelOptionsUtils.getProviderOptionOrDefault(
                            MLModelCommonConstants.ENDPOINT,
                            MLModelSupportedProviders.OPENAI.getDefaultEndpoint());
        } else if (provider == MLModelSupportedProviders.AZUREOPENAI) {
            // Azure OpenAI API doesn't get a default endpoint.
            this.endpoint = modelOptionsUtils.getProviderOption(MLModelCommonConstants.ENDPOINT);
            if (endpoint == null) {
                throw new FlinkRuntimeException(
                        String.format("%s.ENDPOINT setting not found", namespace));
            }
        } else {
            throw new IllegalArgumentException("Unsupported openai provider: " + provider);
        }

        provider.validateEndpoint(endpoint, true);

        if (provider == MLModelSupportedProviders.OPENAI) {
            this.apiKey =
                    secretDecrypterProvider
                            .getMeteredDecrypter(modelOptionsUtils.getEncryptStrategy())
                            .decryptFromKey(OpenAIRemoteModelOptions.API_KEY.key());
        } else {
            this.apiKey =
                    secretDecrypterProvider
                            .getMeteredDecrypter(modelOptionsUtils.getEncryptStrategy())
                            .decryptFromKey(AzureOpenAIRemoteModelOptions.API_KEY.key());
        }

        if (apiKey.isEmpty()) {
            throw new FlinkRuntimeException(
                    String.format("%s.API_KEY setting not found", namespace));
        }
        String inputFormat = getInputFormat(modelOptionsUtils);
        inputFormatter = MLFormatterUtil.getInputFormatter(inputFormat, model);
        String inputContentType =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "input_content_type", inputFormatter.contentType());
        contentType = MediaType.parse(inputContentType);
        String outputFormat = getOutputFormat(modelOptionsUtils, inputFormat);
        outputParser =
                MLFormatterUtil.getOutputParser(outputFormat, model.getOutputSchema().getColumns());
        acceptedContentType =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "output_content_type", outputParser.acceptedContentTypes());
    }

    public static String getInputFormat(ModelOptionsUtils modelOptionsUtils) {
        String defaultInputFormat = "openai-chat";
        // Note that we aren't bothering to pull the real default endpoint here, because the default
        // endpoints also use the default input format.
        String endpoint =
                modelOptionsUtils.getProviderOptionOrDefault(MLModelCommonConstants.ENDPOINT, "");
        if (endpoint.contains("/embeddings")) {
            defaultInputFormat = "openai-embed";
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
        Request.Builder builder = new Request.Builder().url(endpoint).post(getRequestBody(args));
        if (provider == MLModelSupportedProviders.AZUREOPENAI) {
            builder.header(AZURE_AUTH_HEADER, apiKey);
        } else {
            builder.header(AUTHORIZATION_HEADER, "Bearer " + apiKey);
        }
        for (Map.Entry<String, String> header : inputFormatter.headers()) {
            builder.header(header.getKey(), header.getValue());
        }
        builder.header("Accept", acceptedContentType);
        return builder.build();
    }

    @Override
    public Row getContentFromResponse(Response response) {
        return outputParser.parse(response);
    }

    @Override
    public String maskSecrets(String message) {
        return message.replaceAll(apiKey, "*****");
    }

    @Override
    public String getMetricsName() {
        return metricsName;
    }
}
