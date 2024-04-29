/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.http.HttpMethodName;
import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.utils.ModelOptionsUtils;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.ENDPOINT;

/** Implements Model Runtime for AWS Bedrock API. */
public class BedrockProvider implements MLModelRuntimeProvider {
    private final MLModelSupportedProviders supportedProvider = MLModelSupportedProviders.BEDROCK;
    private final CatalogModel model;
    private final transient ObjectMapper mapper = new ObjectMapper();
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final String endpoint;
    private final URL endpointUrl;
    private final URI endpointUri;
    private final AWS4Signer signer = new AWS4Signer();
    private final AWSCredentials credentials;
    private final InputFormatter inputFormatter;
    private final OutputParser outputParser;
    private final MediaType contentType;
    private final String acceptedContentType;
    private final Map<String, String> headers;
    private final SecretDecrypterProvider secretDecrypterProvider;
    private final String metricsName = supportedProvider.getProviderName();

    public BedrockProvider(CatalogModel model, SecretDecrypterProvider secretDecrypterProvider) {
        this.model = model;
        this.secretDecrypterProvider =
                Objects.requireNonNull(secretDecrypterProvider, "SecretDecrypterProvider");
        String namespace = supportedProvider.getProviderName();
        ModelOptionsUtils modelOptionsUtils = new ModelOptionsUtils(model, namespace);
        this.endpoint = modelOptionsUtils.getProviderOption(ENDPOINT);
        if (endpoint == null) {
            throw new FlinkRuntimeException(namespace + ".endpoint setting not found");
        }
        supportedProvider.validateEndpoint(endpoint, true);
        try {
            // if the endpoint didn't end with a slash, add it. This is required by the AWS Signer.
            if (!endpoint.endsWith("/")) {
                this.endpointUrl = URI.create(endpoint + "/").toURL();
            } else {
                this.endpointUrl = URI.create(endpoint).toURL();
            }
            this.endpointUri = endpointUrl.toURI();
        } catch (MalformedURLException e) {
            throw new FlinkRuntimeException("Invalid Bedrock endpoint URL: " + e.getMessage());
        } catch (URISyntaxException e) {
            throw new FlinkRuntimeException("Invalid Bedrock endpoint URI: " + e.getMessage());
        }
        // We sign the request with the access key, secret key, and optionally session token.
        String encryptStrategy = modelOptionsUtils.getEncryptStrategy();
        this.accessKey =
                secretDecrypterProvider
                        .getDecrypter(encryptStrategy)
                        .decryptFromKey(BedrockRemoteModelOptions.ACCESS_KEY_ID.key());
        this.secretKey =
                secretDecrypterProvider
                        .getDecrypter(encryptStrategy)
                        .decryptFromKey(BedrockRemoteModelOptions.SECRET_KEY.key());

        // A session token is optional, but needed for temporary credentials.
        this.sessionToken =
                secretDecrypterProvider
                        .getDecrypter(encryptStrategy)
                        .decryptFromKey(BedrockRemoteModelOptions.SESSION_TOKEN.key());

        if (accessKey.isEmpty() || secretKey.isEmpty()) {
            throw new FlinkRuntimeException(
                    "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required for Bedrock Models");
        }
        if (sessionToken.isEmpty()) {
            this.credentials = new BasicAWSCredentials(accessKey, secretKey);
        } else {
            this.credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
        }
        signer.setServiceName("bedrock");

        // We pick a default format based on the model id in the endpoint, but users can override.
        String inputFormat =
                modelOptionsUtils.getProviderOptionOrDefault(
                        "input_format", pickDefaultFormat(endpoint));
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
        // Precalculate the headers.
        headers = getHeaders();
    }

    private String pickDefaultFormat(String endpoint) {
        // Pull the model id from the endpoint.
        // https://bedrock-runtime.REGION.amazonaws.com/model/MODEL-NAME/invoke
        String modelId = endpoint.split("/model/")[1].split("/")[0];
        // The model id are formatted as PROVIDER.MODEL_NAME, so we can use the provider to pick
        // the default format.
        String provider = modelId.split("\\.")[0].toLowerCase();
        String defaultInputFormat = "AMAZON-TITAN-TEXT";
        switch (provider) {
            case "amazon":
                defaultInputFormat = "AMAZON-TITAN-TEXT";
                break;
            case "ai21":
                defaultInputFormat = "AI21-COMPLETE";
                break;
            case "anthropic":
                // Old models default to ANTHROPIC-COMPLETIONS, all others including claude v3 and
                // anything newer default to ANTHROPIC-MESSAGES.
                if (modelId.equals("anthropic.claude-v2:1")
                        || modelId.equals("anthropic.claude-v2")
                        || modelId.equals("anthropic.claude-instant-v1")) {
                    defaultInputFormat = "ANTHROPIC-COMPLETIONS";
                } else {
                    defaultInputFormat = "ANTHROPIC-MESSAGES";
                }
                break;
            case "cohere":
                defaultInputFormat = "COHERE-GENERATE";
                break;
            case "meta":
                defaultInputFormat = "BEDROCK-LLAMA";
                break;
            case "mistral":
                defaultInputFormat = "MISTRAL-COMPLETIONS";
                break;
        }
        return defaultInputFormat;
    }

    @Override
    public RequestBody getRequestBody(Object[] args) {
        return RequestBody.create(contentType, inputFormatter.format(args));
    }

    public Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", acceptedContentType);
        headers.put("Content-Type", contentType.toString());
        return headers;
    }

    @Override
    public Request getRequest(Object[] args) {
        DefaultRequest request = new DefaultRequest("bedrock");
        request.setEndpoint(endpointUri);
        request.setHttpMethod(HttpMethodName.POST);
        request.setHeaders(headers);

        Request.Builder builder = new Request.Builder().url(endpointUrl);
        byte[] body = inputFormatter.format(args);
        for (Map.Entry<String, String> header : inputFormatter.headers()) {
            request.addHeader(header.getKey(), header.getValue());
        }

        builder.post(RequestBody.create(contentType, body));
        InputStream content = new ByteArrayInputStream(body);
        request.setContent(content);
        // Sign the request. This will set new headers on the request.
        signer.sign(request, credentials);

        // Copy the headers from the signed request to the OkHttp request.
        request.getHeaders().forEach((k, v) -> builder.header((String) k, (String) v));

        return builder.build();
    }

    @Override
    public Row getContentFromResponse(Response response) {
        return outputParser.parse(response);
    }

    @Override
    public String maskSecrets(String message) {
        // None of these ever actually appear in the request/response, but we mask them anyway.
        return message.replaceAll(accessKey, "*****")
                .replaceAll(secretKey, "*****")
                .replaceAll(sessionToken, "*****");
    }

    @Override
    public String getMetricsName() {
        return metricsName;
    }
}
