/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link WatermarkGenerator} that creates watermarks based on a moving histogram of observed
 * delays from the maximum seen timestamp.
 *
 * <p>For efficiency, the histogram algorithm uses a fixed number of buckets with exponentially
 * increasing intervals. A percentile parameter defines how many elements must be included when
 * generating a watermark. The maximum number of elements in the histogram is capped. An
 * exponentially decreasing safety margin is added until the histogram has been fully warmed up.
 */
@Confluent
public class HistogramWatermarkGenerator implements WatermarkGenerator<RowData> {

    /** Mode when to emit watermarks. */
    public enum EmitMode {
        PER_ROW,
        PERIODIC;
    }

    public static final double DEFAULT_PERCENTILE = 0.95;

    public static final int DEFAULT_MAX_CAPACITY = 5000;

    public static final int DEFAULT_MIN_DELAY = 50;

    public static final int DEFAULT_MAX_DELAY = (int) Duration.ofDays(7).toMillis();

    public static final int DEFAULT_BUCKET_TARGET = 100;

    /** Mode when to emit watermarks. */
    private final EmitMode emitMode;

    /** Fraction of elements to be included during watermark calculation. */
    private final double percentile;

    /** Delays considered in the histogram. */
    @VisibleForTesting final int[] delays;

    /** Size (i.e. time interval) of a bucket. */
    @VisibleForTesting final int[] bucketIntervals;

    /** Number of elements contained in a bucket. */
    @VisibleForTesting final int[] bucketCounts;

    /** Next position to be filled in {@link #delays}. */
    @VisibleForTesting int nextDelayPos = 0;

    /** The maximum timestamp encountered so far. */
    @VisibleForTesting long maxTimestamp = Long.MIN_VALUE;

    /** Delay that could cover {@link #percentile} of all records. */
    @VisibleForTesting int percentileDelay = Integer.MAX_VALUE;

    /**
     * Custom constructor for fine-tuning and testing.
     *
     * @param capacity maximum number of delays for "moving" behavior, a high number has an impact
     *     on the memory requirements
     * @param bucketTarget approximate number of buckets, a high number has an impact on the cycles
     *     spend on percentile calculation
     * @param minDelay excludes buckets and thus also watermarks under a certain threshold (e.g.
     *     single digit milliseconds)
     * @param maxDelay the maximum delay for which watermarks will be generated (e.g. not more than
     *     7 days)
     * @param percentile percentage of elements that should be included during watermark calculation
     *     (e.g. 0.95 declares ~5% of the data as late)
     * @param emitMode mode when to emit watermarks, mostly intended for testing purposes.
     */
    public HistogramWatermarkGenerator(
            int capacity,
            int bucketTarget,
            int minDelay,
            int maxDelay,
            double percentile,
            EmitMode emitMode) {
        Preconditions.checkArgument(capacity > 0);
        Preconditions.checkArgument(bucketTarget > 0);
        Preconditions.checkArgument(minDelay > 0);
        Preconditions.checkArgument(maxDelay > 0 && maxDelay < Integer.MAX_VALUE / 2);

        this.emitMode = emitMode;
        this.percentile = percentile;
        delays = new int[capacity];
        Arrays.fill(delays, -1);
        bucketIntervals = assignBuckets(bucketTarget, minDelay, maxDelay);
        bucketCounts = new int[bucketIntervals.length];
    }

    /** Constructor with well-chosen defaults. */
    public HistogramWatermarkGenerator() {
        this(
                DEFAULT_MAX_CAPACITY,
                DEFAULT_BUCKET_TARGET,
                DEFAULT_MIN_DELAY,
                DEFAULT_MAX_DELAY,
                DEFAULT_PERCENTILE,
                EmitMode.PERIODIC);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        final long watermark = maxTimestamp - percentileDelay;
        // Prevent overflows if the maximum timestamp is very low
        if (watermark > maxTimestamp) {
            return;
        }
        output.emitWatermark(new Watermark(watermark));
    }

    @Override
    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
        // 0. Cleansing
        if (eventTimestamp == Long.MIN_VALUE || eventTimestamp == Long.MAX_VALUE) {
            // The timestamps come directly from users, we should protect the system from corner
            // cases for now. If Long.MAX_VALUE is important to them, they can implement a custom
            // watermark strategy.
            return;
        }

        // 1. Advance the maximum timestamp
        if (maxTimestamp == Long.MIN_VALUE) {
            maxTimestamp = eventTimestamp;
            return;
        }

        final long longDelay = Math.max(maxTimestamp - eventTimestamp, 0);
        final int delay = (int) Math.min(longDelay, Integer.MAX_VALUE);
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);

        // 2. Advance the histogram
        bucketCounts[findBucket(delay)] += 1;

        // 3. Handle the "moving part" of the histogram
        // Indicates whether the delay array has been filled with actual values once, if not this
        // needs to be considered in percentile calculation
        final int evictedDelay = addDelay(delay);
        final int totalCount;
        final int safetyMargin;
        if (evictedDelay < 0) {
            // nextDelayPos is 0 when it overflows for the first time
            totalCount = nextDelayPos > 0 ? nextDelayPos : delays.length;
            safetyMargin = findSafetyMargin(totalCount);
        } else {
            bucketCounts[findBucket(evictedDelay)] -= 1;
            totalCount = delays.length;
            safetyMargin = 0;
        }

        // 4. Calculate percentile with safety buffer
        percentileDelay = findPercentile(totalCount) + safetyMargin;

        // 5. Emit watermark immediately if necessary
        if (emitMode == EmitMode.PER_ROW) {
            onPeriodicEmit(output);
        }
    }

    /**
     * Adds the given delay to the {@link #delays} ring buffer.
     *
     * @return evicted element that got replaced by the new one
     */
    private int addDelay(int delay) {
        final int evictedDelay = delays[nextDelayPos];
        delays[nextDelayPos] = delay;
        nextDelayPos = (nextDelayPos + 1) % delays.length;
        return evictedDelay;
    }

    /**
     * Maps a delay to a bucket with an interval that is larger than the delay.
     *
     * @return bucket index
     */
    private int findBucket(int delay) {
        for (int i = 0; i < bucketIntervals.length; i++) {
            if (delay < bucketIntervals[i]) {
                return i;
            }
        }
        return bucketIntervals.length - 1;
    }

    /**
     * Until the histogram is filled with enough data, calculate a safety margin that accounts for
     * uncertainty. It exponentially decreases until the buffer is filled. When the buffer is empty,
     * the safety margin is the highest bucket interval. When the buffer is fully filled, the safety
     * margin is 0.
     */
    private int findSafetyMargin(int totalCount) {
        final double filledPercentage = ((double) totalCount) / delays.length;
        final int safetyBuckets = (int) (bucketIntervals.length * filledPercentage);
        if (safetyBuckets == bucketIntervals.length) {
            return 0;
        }
        return bucketIntervals[bucketIntervals.length - safetyBuckets - 1];
    }

    private int findPercentile(int totalCount) {
        int includedCount = 0;
        int currentBucket = 0;
        while (includedCount < totalCount * percentile) {
            includedCount += bucketCounts[currentBucket];
            currentBucket += 1;
        }
        return bucketIntervals[currentBucket - 1];
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static int[] assignBuckets(int bucketTarget, int minDelay, int maxDelay) {
        final double growthRate = nthRoot(bucketTarget, maxDelay);

        // Deduplicate long tails
        final Set<Integer> expNumbers = new HashSet<>();
        for (int i = 0; expNumbers.size() < bucketTarget; i++) {
            final int expNumber = (int) Math.pow(growthRate, i);
            // Buckets in the order of single digit milliseconds lead to unnecessary overhead
            if (expNumber < minDelay) {
                continue;
            }
            if (expNumber >= maxDelay) {
                break;
            }
            expNumbers.add(expNumber);
        }
        expNumbers.add(maxDelay);

        return expNumbers.stream().sorted().mapToInt(Integer::intValue).toArray();
    }

    private static double nthRoot(int n, double x) {
        return Math.pow(x, 1.0 / (double) n);
    }
}
