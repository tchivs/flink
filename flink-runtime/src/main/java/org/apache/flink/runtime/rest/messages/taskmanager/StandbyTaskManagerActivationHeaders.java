/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.taskmanager.StandbyTaskManagerActivationHandler;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Headers for {@link StandbyTaskManagerActivationHandler}. */
@Confluent
public class StandbyTaskManagerActivationHeaders
        implements RuntimeMessageHeaders<
                StandbyTaskManagerActivationRequestBody,
                EmptyResponseBody,
                EmptyMessageParameters> {
    private static final StandbyTaskManagerActivationHeaders INSTANCE =
            new StandbyTaskManagerActivationHeaders();

    private static final String URL = "/taskmanagers/standby-activation";

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
    public String getDescription() {
        return "Activates standby TaskManager on given address";
    }

    @Override
    public String operationId() {
        return "activateStandbyTaskManager";
    }

    @Override
    public Class<StandbyTaskManagerActivationRequestBody> getRequestClass() {
        return StandbyTaskManagerActivationRequestBody.class;
    }

    @Override
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    public static StandbyTaskManagerActivationHeaders getInstance() {
        return INSTANCE;
    }
}
