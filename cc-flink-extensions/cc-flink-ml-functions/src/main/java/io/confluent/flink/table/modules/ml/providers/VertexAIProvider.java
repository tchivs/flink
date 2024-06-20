/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.providers;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.google.auth.oauth2.GoogleCredentials;
import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.modules.ml.formats.TFServingInputFormatter;
import io.confluent.flink.table.modules.ml.options.VertexAIRemoteModelOptions;
import io.confluent.flink.table.utils.ml.ModelOptionsUtils;
import io.confluent.flink.table.utils.secrets.SecretDecrypterProvider;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

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
    private final SecretDecrypterProvider secretDecrypterProvider;
    private final String metricsName;

    public VertexAIProvider(CatalogModel model, SecretDecrypterProvider secretDecrypterProvider) {
        this.model = model;
        this.secretDecrypterProvider =
                Objects.requireNonNull(secretDecrypterProvider, "SecretDecrypterProvider");

        MLModelSupportedProviders supportedProvider = MLModelSupportedProviders.VERTEXAI;
        final String namespace = supportedProvider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model, namespace);
        String rawEndpoint = modelOptionsUtils.getProviderOption(MLModelCommonConstants.ENDPOINT);
        if (rawEndpoint == null) {
            throw new FlinkRuntimeException(namespace + ".endpoint setting not found");
        }
        supportedProvider.validateEndpoint(rawEndpoint, true);
        Boolean isPublishedModel = rawEndpoint.contains("/publishers/");
        // Pull out the name of the publisher from the endpoint url.
        String publisher = getModelPublisher(rawEndpoint);

        metricsName =
                isPublishedModel
                        ? MLFunctionMetrics.VERTEX_PUB
                        : supportedProvider.getProviderName();

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

        this.endpoint = fixEndpointVerb(rawEndpoint, isPublishedModel, publisher);

        // Vertex AI API doesn't support API KEYs, we use a service account key to authenticate
        // and exchange it for a token.
        String serviceKey =
                secretDecrypterProvider
                        .getMeteredDecrypter(modelOptionsUtils.getEncryptStrategy())
                        .decryptFromKey(VertexAIRemoteModelOptions.SERVICE_KEY.key());
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

    private static String getModelPublisher(String endpoint) {
        Boolean isPublishedModel = endpoint.contains("/publishers/");
        // Pull out the name of the publisher from the endpoint url.
        return isPublishedModel
                ? endpoint.split("/publishers/")[1].split("/")[0].toUpperCase()
                : "";
    }

    public static String getInputFormat(ModelOptionsUtils modelOptionsUtils) {
        String endpoint =
                modelOptionsUtils.getProviderOptionOrDefault(MLModelCommonConstants.ENDPOINT, "");
        return modelOptionsUtils.getProviderOptionOrDefault(
                "input_format", pickDefaultFormat(getModelPublisher(endpoint)));
    }

    public static String getOutputFormat(ModelOptionsUtils modelOptionsUtils, String inputFormat) {
        return modelOptionsUtils.getProviderOptionOrDefault(
                "output_format", MLFormatterUtil.defaultOutputFormat(inputFormat));
    }

    private static String pickDefaultFormat(String publisher) {
        String defaultInputFormat = "tf-serving";
        switch (publisher) {
            case "ANTHROPIC":
                defaultInputFormat = "ANTHROPIC-MESSAGES";
                break;
            case "GOOGLE":
                // TODO: Figure out if it's an embedding model and set the default format
                // accordingly?
                defaultInputFormat = "GEMINI-GENERATE";
                break;
            default:
                defaultInputFormat = "tf-serving";
                break;
        }
        return defaultInputFormat;
    }

    private String fixEndpointVerb(String rawEndpoint, Boolean isPublishedModel, String publisher) {
        if (isPublishedModel) {
            // Google published models use :generateContent instead of :predict
            // Anthropic models use :rawPredict
            // We replace :streamGenerateContent with :generateContent and
            // :streamRawPredict with :rawPredict as we don't want to stream the output.
            rawEndpoint =
                    rawEndpoint
                            .replace(":streamGenerateContent", ":generateContent")
                            .replace(":streamRawPredict", ":rawPredict");
            if (!rawEndpoint.endsWith(":predict")
                    && !rawEndpoint.endsWith(":rawPredict")
                    && !rawEndpoint.endsWith(":generateContent")) {
                switch (publisher.toLowerCase()) {
                    case "anthropic":
                        rawEndpoint = rawEndpoint + ":rawPredict";
                        break;
                    case "google":
                    default:
                        rawEndpoint = rawEndpoint + ":generateContent";
                }
            }
        } else {
            // These models don't support :generateContent, so we replace it with :predict
            rawEndpoint =
                    rawEndpoint
                            .replace(":generateContent", ":predict")
                            .replace(":streamGenerateContent", ":predict");
            // streamRawPredict is not supported, so we replace it with rawPredict
            rawEndpoint = rawEndpoint.replace(":streamRawPredict", ":rawPredict");
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
        }
        return rawEndpoint;
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

    @Override
    public String maskSecrets(String message) {
        // These tokens are short-lived, but we mask it if it hasn't been refreshed.
        return message.replaceAll(getAccessToken(), "*****");
    }

    @Override
    public String getMetricsName() {
        return metricsName;
    }
}
