/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.base.Strings;

import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** This class encapsulates the runtime for model computation. */
public class MLModelRuntime implements AutoCloseable {

    private final transient OkHttpClient httpClient;
    private final MLModelRuntimeProvider provider;
    private final transient MLFunctionMetrics metrics;

    // Clock is used to measure time for metrics. Mockable.
    private Clock clock;

    private MLModelRuntime(
            CatalogModel model, OkHttpClient httpClient, MLFunctionMetrics metrics, Clock clock) {
        this.httpClient = httpClient;
        this.provider = pickProvider(model);
        this.metrics = metrics;
        this.clock = clock;
        metrics.provision(provider.getMetricsName());
    }

    private MLModelRuntime(
            MLModelRuntimeProvider provider,
            OkHttpClient httpClient,
            MLFunctionMetrics metrics,
            Clock clock) {
        this.httpClient = httpClient;
        this.provider = provider;
        this.metrics = metrics;
        this.clock = clock;
        metrics.provision(provider.getMetricsName());
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

    public static MLModelRuntime open(CatalogModel model, MLFunctionMetrics metrics, Clock clock)
            throws Exception {
        final long timeout = Duration.ofSeconds(30).toMillis();
        final OkHttpClient httpClient =
                new OkHttpClient.Builder()
                        .readTimeout(timeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                        .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(timeout, TimeUnit.MILLISECONDS)
                        .addInterceptor(
                                new ModelRetryInterceptor(4, Duration.ofSeconds(2).toMillis()))
                        .build();
        return new MLModelRuntime(model, httpClient, metrics, clock);
    }

    @VisibleForTesting
    public static MLModelRuntime mockOpen(
            CatalogModel model, OkHttpClient httpClient, MLFunctionMetrics metrics, Clock clock) {
        // We don't add the retry interceptor in tests, as that would break the http mock.
        return new MLModelRuntime(model, httpClient, metrics, clock);
    }

    @VisibleForTesting
    public static MLModelRuntime mockOpen(
            MLModelRuntimeProvider provider,
            OkHttpClient httpClient,
            MLFunctionMetrics metrics,
            Clock clock) {
        // We don't add the retry interceptor in tests, as that would break the http mock.
        return new MLModelRuntime(provider, httpClient, metrics, clock);
    }

    /** Interceptor to retry requests on quota errors. */
    public static class ModelRetryInterceptor implements Interceptor {
        private int maxRetries;
        private long retryWait;

        public ModelRetryInterceptor(int maxRetries, long retryWaitMilliseconds) {
            this.maxRetries = maxRetries;
            this.retryWait = retryWaitMilliseconds;
        }

        @Override
        public Response intercept(Chain chain) throws java.io.IOException {
            // Retry the request if the response is a quota or unavailable error, up to maxRetries
            // times, using exponential backoff. All other errors are passed through.
            int retryCount = 0;
            Response response = null;
            while (retryCount <= maxRetries) {
                Request request = chain.request();
                response = chain.proceed(request);
                if ((response.code() == 429 || response.code() == 503) && retryCount < maxRetries) {
                    response.close();
                    // If the response has a retry-after header, we are polite and use that as the
                    // wait time and set this as the last retry.
                    long waitTime = retryWait * (1 << retryCount);
                    if (response.header("Retry-After") != null) {
                        try {
                            // We won't wait more than 60 seconds, regardless of how nicely the
                            // server asks. We also won't wait less than the default wait time.
                            waitTime =
                                    Math.min(
                                            Math.max(
                                                    Long.parseLong(response.header("Retry-After"))
                                                            * 1000,
                                                    retryWait),
                                            Duration.ofSeconds(60).toMillis());
                        } catch (NumberFormatException e) {
                            // If the Retry-After header is not a number, just wait the default and
                            // do one last retry.
                            // The HTTP spec allows it to be a date, which we don't support.
                        }
                        retryCount = maxRetries - 1;
                    }
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {
                        throw new java.io.IOException("Interrupted while waiting to retry", e);
                    }
                    retryCount++;
                } else {
                    return response;
                }
            }
            return response;
        }
    }

    @Override
    public void close() throws Exception {
        this.httpClient.dispatcher().executorService().shutdown();
        this.httpClient.connectionPool().evictAll();
        metrics.deprovision(provider.getMetricsName());
    }

    public String maskInputs(String message, Object[] args) {
        message = provider.maskSecrets(message);
        // Mask any substrings in message that appear in args.
        for (Object arg : args) {
            if (arg instanceof String) {
                message = message.replaceAll((String) arg, "*****");
            }
        }
        // Limit the size of the response string for errors.
        if (message.length() > 300) {
            message = message.substring(0, 300) + "...";
        }
        return message;
    }

    public Row run(Object[] args) throws Exception {
        long startMs = clock.millis();
        final Request request;
        try {
            // The first argument is the model name, which we remove.
            request = provider.getRequest(Arrays.copyOfRange(args, 1, args.length));
        } catch (Throwable t) {
            metrics.requestPrepFailure(provider.getMetricsName());
            // Note: if we hit this case, we don't record a total time, since it will throw off
            // our metrics to have times for cases that don't actually make a request.
            throw t;
        }
        metrics.bytesSent(provider.getMetricsName(), request.body().contentLength());
        metrics.request(provider.getMetricsName());
        long requestStartMs = clock.millis();
        // TODO: This is blocking. We need to make it async.
        try (final Response response = httpClient.newCall(request).execute()) {
            metrics.requestMs(provider.getMetricsName(), clock.millis() - requestStartMs);
            metrics.bytesReceived(provider.getMetricsName(), response.body().contentLength());
            if (!response.isSuccessful()) {
                metrics.requestHttpFailure(provider.getMetricsName(), response.code());
                try (ResponseBody responseBody = response.body()) {
                    String responseString = "";
                    try {
                        if (responseBody != null) {
                            responseString = responseBody.string().trim();
                            // Mask any sensitive information in the response string.
                            responseString = maskInputs(responseString, args);
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
            try {
                Row row = provider.getContentFromResponse(response);
                metrics.requestSuccess(provider.getMetricsName());
                return row;
            } catch (Throwable t) {
                metrics.requestParseFailure(provider.getMetricsName());
                throw t;
            }
        } finally {
            metrics.totalMs(provider.getMetricsName(), clock.millis() - startMs);
        }
    }
}
