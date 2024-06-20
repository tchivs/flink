/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.providers;

import org.apache.flink.types.Row;

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/** Interface for search runtime providers. */
public interface SearchRuntimeProvider {
    public RequestBody getRequestBody(Object[] args);

    public Request getRequest(Object[] args);

    public Row getContentFromResponse(Response response);

    public String maskSecrets(String message);

    public String getMetricsName();
}
