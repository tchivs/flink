/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.utils.MlUtils;
import io.confluent.flink.table.utils.ModelOptionsUtils;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.util.Map;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.API_KEY;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.ENDPOINT;

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

    public AzureMLProvider(CatalogModel model) {
        this.model = model;
        MLModelSupportedProviders supportedProvider = MLModelSupportedProviders.AZUREML;
        String namespace = supportedProvider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model, namespace);
        this.endpoint = modelOptionsUtils.getProviderOption(ENDPOINT);
        supportedProvider.validateEndpoint(endpoint);
        // Azure ML can take either an API Key or an expiring token, but we only support API Key.
        this.apiKey =
                MlUtils.decryptSecret(
                        modelOptionsUtils.getProviderOptionOrDefault(API_KEY, ""),
                        modelOptionsUtils.getEncryptStrategy());
        // The Azure ML Deployment Name is optional, but allows the user to distinguish between
        // two models that are deployed to the same endpoint.
        this.deploymentName = modelOptionsUtils.getProviderOptionOrDefault("deployment_name", "");
        // By default, we use the Pandas DataFrame Split format for input, with Azure ML's slight
        // variation of the top level node name.
        String inputFormat =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "input_format", "azureml-pandas-dataframe");
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
    public Row getContentFromResponse(Response response) {
        return outputParser.parse(response);
    }
}
