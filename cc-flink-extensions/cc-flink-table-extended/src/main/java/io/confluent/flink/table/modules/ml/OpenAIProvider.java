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
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.Map;

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

    public OpenAIProvider(CatalogModel model, MLModelSupportedProviders supportedProvider) {
        this.provider = supportedProvider;
        ModelKind modelKind = ModelOptionsUtils.getModelKind(model.getOptions());
        if (!modelKind.equals(CatalogModel.ModelKind.REMOTE)) {
            throw new FlinkRuntimeException(
                    "For OpenAI, ML Predict expected a remote model, got " + modelKind);
        }
        final String namespace = supportedProvider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model, namespace);

        if (supportedProvider == MLModelSupportedProviders.OPENAI) {
            this.endpoint =
                    modelOptionsUtils.getProviderOptionOrDefault(
                            "ENDPOINT", "https://api.openai.com/v1/chat/completions");
        } else {
            // Azure OpenAI API doesn't get a default endpoint.
            this.endpoint = modelOptionsUtils.getProviderOption("ENDPOINT");
            if (endpoint == null) {
                throw new FlinkRuntimeException(
                        String.format("%s.ENDPOINT setting not found", namespace));
            }
        }
        supportedProvider.validateEndpoint(endpoint);
        this.apiKey = modelOptionsUtils.getProviderOptionOrDefault("API_KEY", "");
        if (apiKey.isEmpty()) {
            throw new FlinkRuntimeException(
                    String.format("%s.API_KEY setting not found", namespace));
        }
        String inputFormat =
                modelOptionsUtils.getProviderOptionOrDefault("input_format", "openai-chat");
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
}
