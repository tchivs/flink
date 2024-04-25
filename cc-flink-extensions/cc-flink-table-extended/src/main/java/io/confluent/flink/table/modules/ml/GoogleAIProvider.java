/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogModel.ModelKind;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.utils.ModelOptionsUtils;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.Map;
import java.util.Objects;

/** Implements Model Runtime for Google AI Studio models, not to be confused with Vertex AI. */
public class GoogleAIProvider implements MLModelRuntimeProvider {
    private final MLModelSupportedProviders supportedProvider = MLModelSupportedProviders.GOOGLEAI;
    private final InputFormatter inputFormatter;
    private final OutputParser outputParser;
    private final MediaType contentType;
    private final String acceptedContentType;
    private final String apiKey;
    private final String endpoint;
    private final SecretDecrypterProvider secretDecrypterProvider;

    public GoogleAIProvider(CatalogModel model, SecretDecrypterProvider secretDecrypterProvider) {
        this.secretDecrypterProvider =
                Objects.requireNonNull(secretDecrypterProvider, "SecretDecrypterProvider");
        final String namespace = supportedProvider.getProviderName();
        final ModelKind modelKind = ModelOptionsUtils.getModelKind(model.getOptions());
        if (!modelKind.equals(CatalogModel.ModelKind.REMOTE)) {
            throw new FlinkRuntimeException(
                    "For GoogleAI, ML Predict expected a remote model, got " + modelKind);
        }
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model, namespace);
        // Pull relevant headers from the model, or use defaults.
        final String urlBase =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "ENDPOINT",
                        "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent");
        supportedProvider.validateEndpoint(urlBase, true);
        this.apiKey =
                secretDecrypterProvider
                        .getDecrypter(modelOptionsUtils.getEncryptStrategy())
                        .decryptFromKey(GoogleAIRemoteModelOptions.API_KEY.key());

        if (apiKey.isEmpty()) {
            // TODO: This exception should fire when the model is created, not when it is run.
            throw new FlinkRuntimeException("Model ML Predict requires an API key");
        }
        // The endpoint is the base URL with the API key added as a query parameter.
        this.endpoint =
                HttpUrl.parse(urlBase)
                        .newBuilder()
                        .addQueryParameter("key", apiKey)
                        .build()
                        .toString();
        String inputFormat =
                modelOptionsUtils.getProviderOptionOrDefault("input_format", "gemini-generate");
        inputFormatter = MLFormatterUtil.getInputFormatter(inputFormat, model);
        String inputContentType =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "input_content_type", inputFormatter.contentType());
        contentType = MediaType.parse(inputContentType);
        String outputFormat =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "output_format", MLFormatterUtil.defaultOutputFormat(inputFormat));
        outputParser =
                MLFormatterUtil.getOutputParser(outputFormat, model.getOutputSchema().getColumns());
        acceptedContentType =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "output_content_type", outputParser.acceptedContentTypes());
    }

    @Override
    public RequestBody getRequestBody(Object[] args) {
        return RequestBody.create(contentType, inputFormatter.format(args));
    }

    @Override
    public Request getRequest(Object[] args) {
        Request.Builder builder = new Request.Builder().url(endpoint).post(getRequestBody(args));
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
}
