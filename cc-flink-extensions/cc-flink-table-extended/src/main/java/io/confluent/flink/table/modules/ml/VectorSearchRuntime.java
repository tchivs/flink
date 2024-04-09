/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.types.Row;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Class implementing VECTOR_SEARCH table function. */
public class VectorSearchRuntime implements AutoCloseable {
    private final transient OkHttpClient httpClient;
    private final MLModelRuntimeProvider provider;

    private VectorSearchRuntime(OkHttpClient httpClient) {
        this.httpClient = httpClient;
        // TODO: Implement Vector Search Providers.
        this.provider = null;
    }

    public static VectorSearchRuntime open() throws Exception {
        final long timeout = Duration.ofSeconds(30).toMillis();
        final OkHttpClient httpClient =
                new OkHttpClient.Builder()
                        .readTimeout(timeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                        .connectTimeout(timeout, TimeUnit.MILLISECONDS)
                        .callTimeout(timeout, TimeUnit.MILLISECONDS)
                        .build();
        return new VectorSearchRuntime(httpClient);
    }

    @Override
    public void close() throws Exception {
        this.httpClient.dispatcher().executorService().shutdown();
        this.httpClient.connectionPool().evictAll();
    }

    public Row run(Object[] args) throws Exception {
        // Args should be of length 3: Table name, top_k, and vector.
        // Remove the table name and get the request.
        final Request request = provider.getRequest(Arrays.copyOfRange(args, 1, args.length));
        // TODO: This is blocking. We need to make it async.
        try (final Response response = httpClient.newCall(request).execute()) {
            // TODO: We should check the response code and deal properly if it's not 2XX.
            return provider.getContentFromResponse(response);
        }
    }
}
