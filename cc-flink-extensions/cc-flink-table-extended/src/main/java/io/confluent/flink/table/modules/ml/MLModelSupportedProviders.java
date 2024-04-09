/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.util.FlinkRuntimeException;

import java.net.URL;

/** enum for supported ML model providers. */
public enum MLModelSupportedProviders {
    // OpenAI endpoints look like https://api.openai.com/v1/chat/completions
    OPENAI("OPENAI", "https://api\\.openai\\.com/.*"),
    // Azure ML endpoints look like https://ENDPOINT.REGION.inference.ml.azure.com/score
    // Azure AI endpoints look like https://ENDPOINT.REGION.inference.ai.azure.com/v1/completions
    // (or /v1/chat/completions)
    AZUREML("AZUREML", "https://[\\w-]+\\.[\\w-]+\\.inference\\.(ml|ai)\\.azure\\.com/.*"),
    // Azure OpenAI endpoints look like
    // https://YOUR_RESOURCE_NAME.openai.azure.com/openai/deployments/YOUR_DEPLOYMENT_NAME/chat/completions?api-version=DATE
    AZUREOPENAI("AZUREOPENAI", "https://[\\w-]+\\.openai\\.azure\\.com/openai/deployments/.+/.*"),
    // AWS Bedrock endpoints look like
    // https://bedrock-runtime.REGION.amazonaws.com/model/MODEL-NAME/invoke
    BEDROCK(
            "BEDROCK",
            "https://bedrock-runtime(-fips)?\\.[\\w-]+\\.amazonaws\\.com/model/.+/invoke"),
    // AWS Sagemaker endpoints look like
    // https://runtime.sagemaker.REGION.amazonaws.com/endpoints/ENDPOINT/invocations
    SAGEMAKER(
            "SAGEMAKER",
            "https://runtime(-fips)?\\.sagemaker\\.[\\w-]+\\.amazonaws\\.com/endpoints/.+/invocations/?"),
    // Google AI endpoints look like
    // https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent
    GOOGLEAI("GOOGLEAI", "https://generativelanguage.googleapis.com/.*"),
    // Vertex AI endpoints look like
    // https://REGION-aiplatform.googleapis.com/v1/projects/PROJECT/locations/LOCATION/endpoints/ENDPOINT:predict
    VERTEXAI(
            "VERTEXAI",
            "https://[\\w-]+-aiplatform\\.googleapis\\.com/v1(beta1)?/projects/.+/locations/.+/endpoints/.+(:predict|:rawPredict)?"),
    ;

    private final String providerName;
    private final String endpointValidation;

    MLModelSupportedProviders(String providerName, String endpointValidation) {
        this.providerName = providerName;
        this.endpointValidation = endpointValidation;
    }

    public String getProviderName() {
        return providerName;
    }

    public void validateEndpoint(String endpoint) {
        // These should be covered by the regex, but for security reasons we explictly check that
        // the endpoints aren't for localhost, confluent.cloud, or any raw IP, and are https.
        if (endpoint.startsWith("http:")) {
            throw new FlinkRuntimeException(
                    String.format(
                            "For %s endpoint expected to be https, got %s",
                            providerName, endpoint));
        }
        URL url;
        try {
            url = new URL(endpoint);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "For %s endpoint expected to be a valid URL, got %s",
                            providerName, endpoint));
        }
        String hostname = url.getHost();
        if (hostname.equals("localhost") || hostname.endsWith(".confluent.cloud")) {
            throw new FlinkRuntimeException(
                    String.format(
                            "For %s endpoint expected to match %s, got %s",
                            providerName, endpointValidation, endpoint));
        }
        // Deny any hostname that looks like an IP address.
        if (hostname.matches("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$")) {
            throw new FlinkRuntimeException(
                    String.format(
                            "For %s endpoint expected to match %s, got %s",
                            providerName, endpointValidation, endpoint));
        }

        if (!endpoint.matches(endpointValidation)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "For %s endpoint expected to match %s, got %s",
                            providerName, endpointValidation, endpoint));
        }
    }
}
