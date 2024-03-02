/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** These headers define the protocol for submitting a Confluent job to a flink cluster. */
@Confluent
public class ConfluentJobSubmitHeaders
        implements RuntimeMessageHeaders<
                ConfluentJobSubmitRequestBody, EmptyResponseBody, EmptyMessageParameters> {

    private static final String URL = "/confluent/submit/compiled-plan";
    private static final ConfluentJobSubmitHeaders INSTANCE = new ConfluentJobSubmitHeaders();

    private ConfluentJobSubmitHeaders() {}

    @Override
    public Class<ConfluentJobSubmitRequestBody> getRequestClass() {
        return ConfluentJobSubmitRequestBody.class;
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
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
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    public static ConfluentJobSubmitHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String getDescription() {
        return "Submits a compiled plan.";
    }

    @Override
    public String operationId() {
        return "submitCompiledPlan";
    }
}
