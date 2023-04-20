/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.handler.job.results.ForegroundResultCoordinationAdapter;

import java.io.IOException;

/**
 * {@link ForegroundResultCoordinationAdapter} for converting from/to {@link
 * CollectCoordinationRequest} and {@link CollectCoordinationResponse}.
 */
@Confluent
@Internal
@SuppressWarnings("unused") // Used via reflection from flink-runtime.
public class DefaultForegroundResultCoordinationAdapter
        implements ForegroundResultCoordinationAdapter {

    @Override
    public CoordinationRequest createRequest(String version, long offset) {
        return new CollectCoordinationRequest(version, offset);
    }

    @Override
    public IntermediateResponse extractIntermediateResponse(CoordinationResponse response) {
        final CollectCoordinationResponse collectResponse = (CollectCoordinationResponse) response;
        return new IntermediateResponse(
                collectResponse.getVersion(),
                collectResponse.getLastCheckpointedOffset(),
                collectResponse.getSerializedResults());
    }

    @Override
    public FinalResponse extractFinalResponse(byte[] acc) throws IOException {
        final Tuple2<Long, CollectCoordinationResponse> collectResponse =
                CollectSinkFunction.deserializeAccumulatorResult(acc);
        return new FinalResponse(
                collectResponse.f1.getVersion(),
                collectResponse.f1.getLastCheckpointedOffset(),
                collectResponse.f1.getSerializedResults(),
                collectResponse.f0);
    }
}
