/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job.results;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.io.IOException;
import java.util.List;

/**
 * Adapter to convert from/to specific {@link CoordinationRequest} and {@link CoordinationResponse}
 * messages located in {@code flink-streaming-java}.
 *
 * <p>This adapter exists for not messing up our Flink fork too much.
 */
@Confluent
public interface ForegroundResultCoordinationAdapter {

    CoordinationRequest createRequest(String version, long offset);

    IntermediateResponse extractIntermediateResponse(CoordinationResponse response);

    FinalResponse extractFinalResponse(byte[] acc) throws IOException;

    /** Response while the coordinator is still reachable. */
    class IntermediateResponse {
        final String version;
        final long lastCheckpointedOffset;
        final List<byte[]> rows;

        public IntermediateResponse(
                String version, long lastCheckpointedOffset, List<byte[]> rows) {
            this.version = version;
            this.lastCheckpointedOffset = lastCheckpointedOffset;
            this.rows = rows;
        }
    }

    /**
     * Response when the coordinator is not reachable anymore. In other words: The job terminated.
     */
    class FinalResponse {
        final String version;
        final long lastCheckpointedOffset;
        final List<byte[]> rows;
        final long offset;

        public FinalResponse(
                String version, long lastCheckpointedOffset, List<byte[]> rows, long offset) {
            this.version = version;
            this.lastCheckpointedOffset = lastCheckpointedOffset;
            this.rows = rows;
            this.offset = offset;
        }
    }
}
