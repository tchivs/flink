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
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_SAFETY_MARGIN;

/**
 * Handles multiple {@link WatermarkGenerator}s with evolving algorithms and fine-tuned parameters.
 */
@Confluent
public class VersionedWatermarkStrategy {
    private static final String DEFAULT_WATERMARK_ALIGNMENT_GROUP = "__default__";
    private static final Duration DEFAULT_ALLOWED_WATERMARK_ALIGNMENT_DRIFT = Duration.ofMinutes(1);

    /** Maps a version to a concrete implementation. */
    public static WatermarkStrategy<RowData> forOptions(WatermarkOptions options) {
        WatermarkStrategy<RowData> strategy;
        switch (options.version) {
            case V0:
                // Fallback in case something is wrong with the default implementation.
                strategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10));
                break;
            case V1:
            case V2:
                strategy =
                        WatermarkStrategy.forGenerator(
                                ctx ->
                                        new HistogramWatermarkGenerator(
                                                DEFAULT_MAX_CAPACITY,
                                                DEFAULT_BUCKET_TARGET,
                                                DEFAULT_MIN_DELAY,
                                                DEFAULT_MAX_DELAY,
                                                DEFAULT_PERCENTILE,
                                                DEFAULT_SAFETY_MARGIN,
                                                options.emitPerRow
                                                        ? EmitMode.PER_ROW
                                                        : EmitMode.PERIODIC));
                break;
            default:
                throw new IllegalArgumentException("Unknown watermark generator version.");
        }
        if (options.version == ConfluentManagedTableOptions.SourceWatermarkVersion.V2) {
            strategy =
                    strategy.withWatermarkAlignment(
                            DEFAULT_WATERMARK_ALIGNMENT_GROUP,
                            DEFAULT_ALLOWED_WATERMARK_ALIGNMENT_DRIFT);
        }
        if (options.idleTimeout.isZero() || options.idleTimeout.isNegative()) {
            return strategy;
        } else {
            return strategy.withIdleness(options.idleTimeout);
        }
    }

    private VersionedWatermarkStrategy() {
        // No instantiation
    }
}
