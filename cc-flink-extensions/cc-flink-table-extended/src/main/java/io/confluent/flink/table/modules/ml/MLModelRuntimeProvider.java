/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.types.Row;

import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/** Encapsulates the runtime for model computation for a single model provider. */
public interface MLModelRuntimeProvider {
    public RequestBody getRequestBody(Object[] args);

    public Request getRequest(Object[] args);

    public Row getContentFromResponse(Response response);

    public String maskSecrets(String message);

    public String getMetricsName();
}
