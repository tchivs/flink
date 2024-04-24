/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.http.HttpMethodName;
import io.confluent.flink.table.modules.ml.formats.DataSerializer;
import io.confluent.flink.table.modules.ml.formats.InputFormatter;
import io.confluent.flink.table.modules.ml.formats.MLFormatterUtil;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.utils.ModelOptionsUtils;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.ENDPOINT;

/** Implements Model Runtime for Sagemaker API. */
public class SagemakerProvider implements MLModelRuntimeProvider {
    private final CatalogModel model;
    private final transient ObjectMapper mapper = new ObjectMapper();
    private final String accessKey;
    private final String secretKey;
    private final String sessionToken;
    private final String endpoint;
    private final URL endpointUrl;
    private final URI endpointUri;
    private final String targetVariant;
    private final String targetModel;
    private final String targetContainerHostname;
    private final String inferenceComponentName;
    private final String customAttribute;
    private final String inferenceId;
    private final String enableExplanations;
    private final AWS4Signer signer = new AWS4Signer();
    private final AWSCredentials credentials;
    private final InputFormatter inputFormatter;
    private final OutputParser outputParser;
    private final MediaType contentType;
    private final String acceptedContentType;
    private final Map<String, String> headers;
    private final SecretDecrypterProvider secretDecrypterProvider;

    public SagemakerProvider(CatalogModel model, SecretDecrypterProvider secretDecrypterProvider) {
        this.model = model;
        this.secretDecrypterProvider =
                Objects.requireNonNull(secretDecrypterProvider, "SecretDecrypterProvider");

        MLModelSupportedProviders supportedProvider = MLModelSupportedProviders.SAGEMAKER;
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
            throw new FlinkRuntimeException("Invalid Sagemaker endpoint URL: " + e.getMessage());
        } catch (URISyntaxException e) {
            throw new FlinkRuntimeException("Invalid Sagemaker endpoint URI: " + e.getMessage());
        }
        // We sign the request with the access key, secret key, and optionally session token.
        String encryptStrategy = modelOptionsUtils.getEncryptStrategy();
        this.accessKey =
                secretDecrypterProvider
                        .getDecrypter(encryptStrategy)
                        .decryptFromKey(SageMakerRemoteModelOptions.ACCESS_KEY_ID.key());
        this.secretKey =
                secretDecrypterProvider
                        .getDecrypter(encryptStrategy)
                        .decryptFromKey(SageMakerRemoteModelOptions.SECRET_KEY.key());

        // A session token is optional, but needed for temporary credentials.
        this.sessionToken =
                secretDecrypterProvider
                        .getDecrypter(encryptStrategy)
                        .decryptFromKey(SageMakerRemoteModelOptions.SESSION_TOKEN.key());

        if (accessKey.isEmpty() || secretKey.isEmpty()) {
            throw new FlinkRuntimeException(
                    "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required for Sagemaker Models");
        }
        if (sessionToken.isEmpty()) {
            this.credentials = new BasicAWSCredentials(accessKey, secretKey);
        } else {
            this.credentials = new BasicSessionCredentials(accessKey, secretKey, sessionToken);
        }
        signer.setServiceName("sagemaker");
        // The custom attribute is optional and passed as a header to the Sagemaker endpoint.
        // We currently only support a static custom attribute, but it could be made dynamic by
        // linking it to a column in the input schema.
        this.customAttribute = modelOptionsUtils.getProviderOptionOrDefault("CUSTOM_ATTRIBUTE", "");
        // The inference ID is logged by Sagemaker and can be used to track requests.
        // We're currently only supporting a static one.
        this.inferenceId = modelOptionsUtils.getProviderOptionOrDefault("INFERENCE_ID", "");
        // The Target Variant is optional, but allows the user to distinguish between
        // two a/b testing variants of a single models that are deployed to the same endpoint.
        this.targetVariant = modelOptionsUtils.getProviderOptionOrDefault("TARGET_VARIANT", "");
        // The Target Model allows the user to distinguish between two models that are deployed to
        // the same endpoint, and is totally different from the Target Variant, despite the fact
        // that you can use both of them to do the same thing.
        this.targetModel = modelOptionsUtils.getProviderOptionOrDefault("TARGET_MODEL", "");
        // The Target Container Hostname is yet a third way to use the same endpoint to call
        // different models, and is totally different from the Target Model and Target Variant.
        // This is such a good design by Sagemaker. So flexible, right?
        this.targetContainerHostname =
                modelOptionsUtils.getProviderOptionOrDefault("TARGET_CONTAINER_HOSTNAME", "");
        // The Inference Component Name is a fourth way to use the same endpoint to call different
        // models, and is different from the other three in a way that is not immediately obvious.
        // Unfortunately, this comment is not long enough to explain it. Basically we just let the
        // user specify a string and pass it to Sagemaker in a header.
        this.inferenceComponentName =
                modelOptionsUtils.getProviderOptionOrDefault("INFERENCE_COMPONENT_NAME", "");
        // The Enable Explanations setting allows explanations to be returned with the predictions.
        // We can't read the explanations yet, so we would turn it off by default, except that
        // Sagemaker will throw an error if you turn it off when it's not supported by the model.
        // Note that this is a JMESPath expression string, not a boolean.
        this.enableExplanations =
                modelOptionsUtils.getProviderOptionOrDefault("ENABLE_EXPLANATIONS", "");

        String inputFormat = modelOptionsUtils.getProviderOptionOrDefault("input_format", "csv");
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

    private static Object getRowFieldFromJson(
            JsonNode node, LogicalType dataType, DataSerializer.OutputDeserializer converter)
            throws IOException {
        if (node.isArray() && node.size() == 1 && !dataType.is(LogicalTypeRoot.ARRAY)) {
            // Take the first element of the array.
            node = node.get(0);
        }
        return converter.convert(node);
    }

    @Override
    public RequestBody getRequestBody(Object[] args) {
        return RequestBody.create(contentType, inputFormatter.format(args));
    }

    public Map<String, String> getHeaders() {
        Map<String, String> headers = new HashMap<>();
        if (!targetVariant.isEmpty()) {
            headers.put("X-Amzn-SageMaker-Target-Variant", targetVariant);
        }
        if (!targetModel.isEmpty()) {
            headers.put("X-Amzn-SageMaker-Target-Model", targetModel);
        }
        if (!targetContainerHostname.isEmpty()) {
            headers.put("X-Amzn-SageMaker-Target-Container-Hostname", targetContainerHostname);
        }
        if (!inferenceComponentName.isEmpty()) {
            headers.put("X-Amzn-SageMaker-Inference-Component", inferenceComponentName);
        }
        if (!customAttribute.isEmpty()) {
            headers.put("X-Amzn-SageMaker-Custom-Attributes", customAttribute);
        }
        if (!inferenceId.isEmpty()) {
            headers.put("X-Amzn-SageMaker-Inference-Id", inferenceId);
        }
        if (!enableExplanations.isEmpty()) {
            headers.put("X-Amzn-SageMaker-Enable-Explanations", enableExplanations);
        }
        headers.put("Accept", acceptedContentType);
        return headers;
    }

    @Override
    public Request getRequest(Object[] args) {
        DefaultRequest request = new DefaultRequest("sagemaker");
        request.setEndpoint(endpointUri);
        request.setHttpMethod(HttpMethodName.POST);
        request.setHeaders(headers);

        Request.Builder builder = new Request.Builder().url(endpointUrl);
        byte[] body = inputFormatter.format(args);
        // Sagemaker doesn't allow any custom headers to be set on the request, so any important
        // ones (i.e. Triton json length) need to be shoved into the content type.
        String requestContentType = contentType.toString();
        for (Map.Entry<String, String> header : inputFormatter.headers()) {
            if (header.getKey().equals("Inference-Header-Content-Length")) {
                requestContentType =
                        "application/vnd.sagemaker-triton.binary+json;json-header-size="
                                + header.getValue();
            }
        }
        headers.put("Content-Type", requestContentType);
        builder.post(RequestBody.create(MediaType.parse(requestContentType), body));
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
}
