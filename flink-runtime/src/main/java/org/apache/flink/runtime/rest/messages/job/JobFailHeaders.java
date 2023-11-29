/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for the {@link org.apache.flink.runtime.rest.handler.job.JobFailHandler}. */
public class JobFailHeaders
        implements RuntimeMessageHeaders<
                JobFailRequestBody, EmptyResponseBody, JobMessageParameters> {

    public static final String URL = "/jobs/:jobid/fail";

    private static final JobFailHeaders INSTANCE = new JobFailHeaders();

    private JobFailHeaders() {}

    @Override
    public Class<JobFailRequestBody> getRequestClass() {
        return JobFailRequestBody.class;
    }

    @Override
    public Class<EmptyResponseBody> getResponseClass() {
        return EmptyResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.ACCEPTED;
    }

    @Override
    public JobMessageParameters getUnresolvedMessageParameters() {
        return new JobMessageParameters();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.PATCH;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    public static JobFailHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Fail a job.";
    }

    @Override
    public String operationId() {
        return "failJob";
    }
}
