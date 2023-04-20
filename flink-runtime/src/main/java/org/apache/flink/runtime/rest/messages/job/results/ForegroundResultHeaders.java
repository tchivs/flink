/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.job.results;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** {@link RuntimeMessageHeaders} to retrieve foreground query results. */
@Confluent
public class ForegroundResultHeaders
        implements RuntimeMessageHeaders<
                ForegroundResultRequestBody,
                ForegroundResultResponseBody,
                ForegroundResultMessageParameters> {

    public static final String URL = "/jobs/:jobid/foreground-result/:operatorid";

    private static final ForegroundResultHeaders INSTANCE = new ForegroundResultHeaders();

    private ForegroundResultHeaders() {}

    @Override
    public Class<ForegroundResultRequestBody> getRequestClass() {
        return ForegroundResultRequestBody.class;
    }

    @Override
    public Class<ForegroundResultResponseBody> getResponseClass() {
        return ForegroundResultResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public ForegroundResultMessageParameters getUnresolvedMessageParameters() {
        return new ForegroundResultMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static ForegroundResultHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Sends a request to a specified coordinator of the specified job for foreground query results. "
                + "This API is for internal use only.";
    }
}
