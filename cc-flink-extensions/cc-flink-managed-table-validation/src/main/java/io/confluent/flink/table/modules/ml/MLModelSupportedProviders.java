/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.FlinkRuntimeException;

import java.net.URL;
import java.util.Locale;

/** enum for supported ML model providers. */
public enum MLModelSupportedProviders {
    // OpenAI endpoints look like https://api.openai.com/v1/chat/completions
    OPENAI("OPENAI", "https://api\\.openai\\.com/.*", "https://api.openai.com/v1/chat/completions"),
    // Azure ML endpoints look like https://ENDPOINT.REGION.inference.ml.azure.com/score
    // Azure AI endpoints look like https://ENDPOINT.REGION.inference.ai.azure.com/v1/completions
    // (or /v1/chat/completions)
    AZUREML("AZUREML", "https://[\\w-]+\\.[\\w-]+\\.inference\\.(ml|ai)\\.azure\\.com/.*", ""),
    // Azure OpenAI endpoints look like
    // https://YOUR_RESOURCE_NAME.openai.azure.com/openai/deployments/YOUR_DEPLOYMENT_NAME/chat/completions?api-version=DATE
    AZUREOPENAI(
            "AZUREOPENAI", "https://[\\w-]+\\.openai\\.azure\\.com/openai/deployments/.+/.*", ""),
    // AWS Bedrock endpoints look like
    // https://bedrock-runtime.REGION.amazonaws.com/model/MODEL-NAME/invoke
    BEDROCK(
            "BEDROCK",
            "https://bedrock-runtime(-fips)?\\.[\\w-]+\\.amazonaws\\.com/model/.+/invoke/?",
            ""),
    // AWS Sagemaker endpoints look like
    // https://runtime.sagemaker.REGION.amazonaws.com/endpoints/ENDPOINT/invocations
    SAGEMAKER(
            "SAGEMAKER",
            "https://runtime(-fips)?\\.sagemaker\\.[\\w-]+\\.amazonaws\\.com/endpoints/.+/invocations/?",
            ""),
    // Google AI endpoints look like
    // https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent
    GOOGLEAI(
            "GOOGLEAI",
            "https://generativelanguage.googleapis.com/.*",
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"),
    // Vertex AI endpoints look like
    // https://REGION-aiplatform.googleapis.com/v1/projects/PROJECT/locations/LOCATION/endpoints/ENDPOINT:predict
    // Published Generative AI model endpoints look like
    // https://REGION-aiplatform.googleapis.com/v1/projects/PROJECT/locations/LOCATION/publishers/PUBLISHER/models/MODEL:generateContent
    VERTEXAI(
            "VERTEXAI",
            "https://[\\w-]+-aiplatform\\.googleapis\\.com/v1(beta1)?/projects/.+/locations/.+/(endpoints|publishers)/.+(:predict|:rawPredict|:generateContent|:streamGenerateContent|:streamRawPredict)?",
            ""),
    ;

    private final String providerName;
    private final String endpointValidation;
    private final String defaultEndpoint;

    MLModelSupportedProviders(
            String providerName, String endpointValidation, String defaultEndpoint) {
        this.providerName = providerName;
        this.endpointValidation = endpointValidation;
        this.defaultEndpoint = defaultEndpoint;
    }

    public String getProviderName() {
        return providerName;
    }

    public String getEndpointValidation() {
        return endpointValidation;
    }

    public String getDefaultEndpoint() {
        return defaultEndpoint;
    }

    public void validateEndpoint(String endpoint, boolean runtime) {
        if (endpoint == null) {
            final String msg = String.format("For %s, endpoint is required", providerName);
            if (runtime) {
                throw new FlinkRuntimeException(msg);
            } else {
                throw new ValidationException(msg);
            }
        }
        // These should be covered by the regex, but for security reasons we explictly check that
        // the endpoints aren't for localhost, confluent.cloud, or any raw IP, and are https.
        if (endpoint.startsWith("http:")) {
            final String msg =
                    String.format(
                            "For %s endpoint, the protocol should be https, got %s",
                            providerName, endpoint);
            if (runtime) {
                throw new FlinkRuntimeException(msg);
            } else {
                throw new ValidationException(msg);
            }
        }

        String errorMsg =
                String.format(
                        "For %s endpoint expected to match %s, got %s",
                        providerName, endpointValidation, endpoint);
        URL url = null;
        try {
            url = new URL(endpoint);
        } catch (Exception e) {
            throwEndpointValidationException(runtime, errorMsg, e);
        }
        String hostname = url.getHost();
        if (hostname.equals("localhost") || hostname.endsWith(".confluent.cloud")) {
            throwEndpointValidationException(runtime, errorMsg);
        }
        // Deny any hostname that looks like an IP address.
        if (hostname.matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$")) {
            throwEndpointValidationException(runtime, errorMsg);
        }

        if (!endpoint.matches(endpointValidation)) {
            throwEndpointValidationException(runtime, errorMsg);
        }
    }

    public static MLModelSupportedProviders fromString(String providerName) {
        return MLModelSupportedProviders.valueOf(providerName.toUpperCase(Locale.ROOT));
    }

    private void throwEndpointValidationException(boolean runtime, String msg) {
        throwEndpointValidationException(runtime, msg, null);
    }

    private void throwEndpointValidationException(boolean runtime, String msg, Exception e) {
        if (runtime) {
            if (e == null) {
                throw new FlinkRuntimeException(msg);
            }
            throw new FlinkRuntimeException(msg, e);
        }
        if (e == null) {
            throw new ValidationException(msg);
        }
        throw new ValidationException(msg, e);
    }
}
