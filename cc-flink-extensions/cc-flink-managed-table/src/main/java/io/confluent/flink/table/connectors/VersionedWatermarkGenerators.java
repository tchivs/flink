/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.table.data.RowData;

import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_BUCKET_TARGET;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MAX_CAPACITY;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MAX_DELAY;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MIN_DELAY;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_PERCENTILE;

/**
 * Prepares the stack for multiple {@link WatermarkGenerator}s with evolving algorithms and
 * fine-tuned parameters.
 */
@Confluent
public class VersionedWatermarkGenerators {

    public static final int V1 = 1;

    public static WatermarkGenerator<RowData> forVersion(int version) {
        switch (version) {
            case V1:
                return new HistogramWatermarkGenerator(
                        DEFAULT_MAX_CAPACITY,
                        DEFAULT_BUCKET_TARGET,
                        DEFAULT_MIN_DELAY,
                        DEFAULT_MAX_DELAY,
                        DEFAULT_PERCENTILE);
            default:
                throw new IllegalArgumentException("Unknown watermark generator version.");
        }
    }

    private VersionedWatermarkGenerators() {
        // No instantiation
    }
}
