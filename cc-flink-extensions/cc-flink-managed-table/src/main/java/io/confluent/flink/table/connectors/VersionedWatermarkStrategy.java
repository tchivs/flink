/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;

import io.confluent.flink.table.connectors.ConfluentManagedTableUtils.WatermarkOptions;
import io.confluent.flink.table.connectors.HistogramWatermarkGenerator.EmitMode;

import java.time.Duration;

import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_BUCKET_TARGET;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MAX_CAPACITY;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MAX_DELAY;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MIN_DELAY;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_PERCENTILE;

/**
 * Handles multiple {@link WatermarkGenerator}s with evolving algorithms and fine-tuned parameters.
 */
@Confluent
public class VersionedWatermarkStrategy {

    /** Maps a version to a concrete implementation. */
    public static WatermarkStrategy<RowData> forOptions(WatermarkOptions options) {
        switch (options.version) {
            case V0:
                // Fallback in case something is wrong with the default implementation.
                return WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));
            case V1:
                return WatermarkStrategy.forGenerator(
                        ctx ->
                                new HistogramWatermarkGenerator(
                                        DEFAULT_MAX_CAPACITY,
                                        DEFAULT_BUCKET_TARGET,
                                        DEFAULT_MIN_DELAY,
                                        DEFAULT_MAX_DELAY,
                                        DEFAULT_PERCENTILE,
                                        options.emitPerRow ? EmitMode.PER_ROW : EmitMode.PERIODIC));
            default:
                throw new IllegalArgumentException("Unknown watermark generator version.");
        }
    }

    private VersionedWatermarkStrategy() {
        // No instantiation
    }
}
