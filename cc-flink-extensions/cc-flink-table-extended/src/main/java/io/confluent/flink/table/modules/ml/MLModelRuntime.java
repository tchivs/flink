/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import io.confluent.flink.table.utils.ModelOptionsUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** This class encapsulates the runtime for model computation. */
public class MLModelRuntime implements AutoCloseable {

    private final transient OkHttpClient httpClient;
    private final MLModelRuntimeProvider provider;

    private MLModelRuntime(CatalogModel model, OkHttpClient httpClient) {
        this.httpClient = httpClient;
        this.provider = pickProvider(model);
    }

    private MLModelRuntimeProvider pickProvider(CatalogModel model) {
        if (model == null) {
            return null;
        }
        // TODO: validate the supported options during model creation time,
        // and store the valid options including the default options in the model metadata
        String modelProvider = ModelOptionsUtils.getProvider(model.getOptions());
        if (modelProvider.isEmpty()) {
            throw new FlinkRuntimeException("Model PROVIDER option not specified");
        }

        final SecretDecrypterProvider secretDecrypterProvider =
                new SecretDecrypterProviderImpl(model);

        if (modelProvider.equalsIgnoreCase(MLModelSupportedProviders.OPENAI.getProviderName())) {
            // OpenAI through their own API, not to be confused with the Azure OpenAI API.
            return new OpenAIProvider(
                    model, MLModelSupportedProviders.OPENAI, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.GOOGLEAI.getProviderName())) {
            // Any of the Google AI models through their makersuite or generativeapi endpoints.
            return new GoogleAIProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.VERTEXAI.getProviderName())) {
            // GCP Vertex AI models.
            return new VertexAIProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.SAGEMAKER.getProviderName())) {
            // AWS Sagemaker models.
            return new SagemakerProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.BEDROCK.getProviderName())) {
            // AWS Bedrock models.
            return new BedrockProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.AZUREML.getProviderName())) {
            // Azure ML models.
            return new AzureMLProvider(model, secretDecrypterProvider);
        } else if (modelProvider.equalsIgnoreCase(
                MLModelSupportedProviders.AZUREOPENAI.getProviderName())) {
            // Azure Open AI is just a special case of Open AI.
            return new OpenAIProvider(
                    model, MLModelSupportedProviders.AZUREOPENAI, secretDecrypterProvider);
        } else {
            throw new UnsupportedOperationException(
                    "Model provider not supported: " + modelProvider);
        }
    }

    public static MLModelRuntime open(CatalogModel model) throws Exception {
        final long timeout = Duration.ofSeconds(30).toMillis();
        final OkHttpClient httpClient =
                new OkHttpClient.Builder()
                        .readTimeout(timeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                        .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(timeout, TimeUnit.MILLISECONDS)
                        .build();
        return new MLModelRuntime(model, httpClient);
    }

    @Override
    public void close() throws Exception {
        this.httpClient.dispatcher().executorService().shutdown();
        this.httpClient.connectionPool().evictAll();
    }

    public Row run(Object[] args) throws Exception {
        // The first argument is the model name, which we remove.
        final Request request = provider.getRequest(Arrays.copyOfRange(args, 1, args.length));
        // TODO: This is blocking. We need to make it async.
        try (final Response response = httpClient.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                try (ResponseBody responseBody = response.body()) {
                    String responseString = "";
                    try {
                        if (responseBody != null) {
                            responseString = responseBody.string().trim();
                        }
                    } catch (Exception e) {
                        // ignored.
                    }

                    throw new FlinkRuntimeException(
                            String.format(
                                    "Received bad response code %d message %s, response: %s",
                                    response.code(),
                                    Strings.isNullOrEmpty(response.message())
                                            ? "<no message>"
                                            : response.message(),
                                    Strings.isNullOrEmpty(responseString)
                                            ? "<no body>"
                                            : responseString));
                }
            }
            return provider.getContentFromResponse(response);
        }
    }
}
