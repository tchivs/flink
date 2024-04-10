/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.google.auth.oauth2.GoogleCredentials;
import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.modules.ml.formats.TFServingInputFormatter;
import io.confluent.flink.table.utils.MlUtils;
import io.confluent.flink.table.utils.ModelOptionsUtils;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/** Implements Model Runtime for GCP Vertex AI. */
public class VertexAIProvider implements MLModelRuntimeProvider {
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private final CatalogModel model;
    private transient ObjectMapper mapper = new ObjectMapper();
    private final String endpoint;
    private final GoogleCredentials credentials;
    private final InputFormatter inputFormatter;
    private final OutputParser outputParser;
    private final MediaType contentType;
    private final String acceptedContentType;

    public VertexAIProvider(CatalogModel model) {
        this.model = model;
        MLModelSupportedProviders supportedProvider = MLModelSupportedProviders.VERTEXAI;
        final String namespace = supportedProvider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model, namespace);
        String rawEndpoint = modelOptionsUtils.getProviderOption("ENDPOINT");
        if (rawEndpoint == null) {
            throw new FlinkRuntimeException(namespace + ".endpoint setting not found");
        }
        supportedProvider.validateEndpoint(rawEndpoint);
        String inputFormat =
                modelOptionsUtils.getProviderOptionOrDefault("input_format", "tf-serving");
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

        // if the raw endpoint doesn't end in :predict or :rawPredict, we add :predict
        // for TF Serving models, and :rawPredict for other inputs. We won't override the user's
        // choice of rawPredict, but we will switch predict to rawPredict if necessary.
        if (!rawEndpoint.endsWith(":predict") && !rawEndpoint.endsWith(":rawPredict")) {
            rawEndpoint = rawEndpoint + ":predict";
        }
        Boolean isTfServing = inputFormatter instanceof TFServingInputFormatter;
        if (!isTfServing && rawEndpoint.endsWith(":predict")) {
            rawEndpoint = rawEndpoint.replace(":predict", ":rawPredict");
        }
        this.endpoint = rawEndpoint;

        // Vertex AI API doesn't support API KEYs, we use a service account key to authenticate
        // and exchange it for a token.
        String serviceKey =
                MlUtils.decryptSecret(
                        modelOptionsUtils.getProviderOptionOrDefault("SERVICE_KEY", ""),
                        modelOptionsUtils.isEncryptStrategyPlaintext());
        if (serviceKey.isEmpty()) {
            throw new FlinkRuntimeException(
                    "For Vertex AI model, SERVICE_KEY is required to authenticate the client");
        }
        try {
            credentials = getCredentials(serviceKey);
            getAccessToken();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create client for Vertex AI endpoint: " + e);
        }
    }

    @VisibleForTesting
    public GoogleCredentials getCredentials(String serviceKey) throws IOException {
        return GoogleCredentials.fromStream(new ByteArrayInputStream(serviceKey.getBytes()))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
    }

    @VisibleForTesting
    public String getAccessToken() {
        try {
            credentials.refreshIfExpired();
        } catch (IOException e) {
            throw new RuntimeException("Unable to refresh credentials for Vertex AI: " + e);
        }
        return credentials.getAccessToken().getTokenValue();
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
                        .header(AUTHORIZATION_HEADER, "Bearer " + getAccessToken());
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
