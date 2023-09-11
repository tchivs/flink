/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import io.confluent.flink.table.connectors.HistogramWatermarkGenerator.EmitMode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_BUCKET_TARGET;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MAX_CAPACITY;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MAX_DELAY;
import static io.confluent.flink.table.connectors.HistogramWatermarkGenerator.DEFAULT_MIN_DELAY;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HistogramWatermarkGenerator}. */
@Confluent
public class HistogramWatermarkGeneratorTest {

    @Test
    void testNoDelay() {
        final int capacity = 10;
        final int bucketTarget = 10;
        final int minDelay = 50;
        final int maxDelay = 1000;
        final double percentile = 1.0;
        final HistogramWatermarkGenerator g =
                new HistogramWatermarkGenerator(
                        capacity, bucketTarget, minDelay, maxDelay, percentile, EmitMode.PERIODIC);

        // Initial state
        assertThat(g.bucketIntervals).containsExactly(63, 125, 251, 501, 1000);
        assertThat(g.maxTimestamp).isEqualTo(Long.MIN_VALUE);
        assertThat(g.bucketCounts).containsExactly(0, 0, 0, 0, 0);
        assertThat(g.percentileDelay).isEqualTo(Integer.MAX_VALUE);

        // Init timestamp=10
        g.onEvent(null, 10, null);
        assertThat(g.maxTimestamp).isEqualTo(10L);
        assertThat(g.percentileDelay).isEqualTo(Integer.MAX_VALUE);

        // timestamp=15, delay=0
        g.onEvent(null, 15, null);
        assertThat(g.maxTimestamp).isEqualTo(15L);
        // lowest interval + the highest safety margin
        assertThat(g.percentileDelay).isEqualTo(1063);

        // timestamp=20, delay=0
        g.onEvent(null, 20, null);
        // highest interval + reduced safety margin
        assertThat(g.percentileDelay).isEqualTo(564);

        // timestamp=21 till 29, delay=0
        IntStream.range(0, 9).forEach(i -> g.onEvent(null, 21 + i, null));
        // lowest interval + no safety margin
        assertThat(g.percentileDelay).isEqualTo(63);
    }

    @Test
    void testWithDelay() {
        final int capacity = 10;
        final int bucketTarget = 10;
        final int minDelay = 50;
        final int maxDelay = 1000;
        final double percentile = 1.0;
        final HistogramWatermarkGenerator g =
                new HistogramWatermarkGenerator(
                        capacity, bucketTarget, minDelay, maxDelay, percentile, EmitMode.PERIODIC);

        // Initial state
        assertThat(g.bucketIntervals).containsExactly(63, 125, 251, 501, 1000);
        assertThat(g.maxTimestamp).isEqualTo(Long.MIN_VALUE);
        assertThat(g.bucketCounts).containsExactly(0, 0, 0, 0, 0);
        assertThat(g.percentileDelay).isEqualTo(Integer.MAX_VALUE);

        // Init timestamp=10
        g.onEvent(null, 10, null);
        assertThat(g.maxTimestamp).isEqualTo(10L);
        assertThat(g.percentileDelay).isEqualTo(Integer.MAX_VALUE);

        // timestamp=400, delay=0
        g.onEvent(null, 400, null);
        assertThat(g.bucketCounts).containsExactly(1, 0, 0, 0, 0);
        // lowest interval + the highest safety margin
        assertThat(g.percentileDelay).isEqualTo(1063);

        // timestamp=200, delay=200
        g.onEvent(null, 200, null);
        assertThat(g.bucketCounts).containsExactly(1, 0, 1, 0, 0);
        // higher interval (251) + lower safety margin (501)
        assertThat(g.percentileDelay).isEqualTo(752);

        // timestamp=100, delay=300
        g.onEvent(null, 100, null);
        assertThat(g.bucketCounts).containsExactly(1, 0, 1, 1, 0);
        // higher interval (501) + lower safety margin (501)
        assertThat(g.percentileDelay).isEqualTo(1002);

        // timestamp=392-399, delay=1
        IntStream.range(0, 7).forEach(i -> g.onEvent(null, 392 + i, null));
        assertThat(g.bucketCounts).containsExactly(8, 0, 1, 1, 0);
        // final interval (501) + no safety margin (0)
        assertThat(g.percentileDelay).isEqualTo(501);
    }

    @Test
    void testWithHighRandomDelay() {
        final double percentile = 1.0;
        final HistogramWatermarkGenerator g =
                new HistogramWatermarkGenerator(
                        DEFAULT_MAX_CAPACITY,
                        DEFAULT_BUCKET_TARGET,
                        DEFAULT_MIN_DELAY,
                        DEFAULT_MAX_DELAY,
                        percentile,
                        EmitMode.PERIODIC);

        final ThreadLocalRandom r = ThreadLocalRandom.current();
        long clock = 0;
        for (int i = 0; i < DEFAULT_MAX_CAPACITY * 2; i++) {
            clock++;
            g.onEvent(null, clock + r.nextInt(-2000, 2000), null);
        }
        // The maximum delay is 4000 which translates to the 4878 bucket interval
        assertThat(g.percentileDelay).isLessThanOrEqualTo(4878);
    }

    @Test
    void testWithHighNumberOfDelaysAnd95Percentile() {
        final double percentile = 0.95;
        final HistogramWatermarkGenerator g =
                new HistogramWatermarkGenerator(
                        DEFAULT_MAX_CAPACITY,
                        DEFAULT_BUCKET_TARGET,
                        DEFAULT_MIN_DELAY,
                        DEFAULT_MAX_DELAY,
                        percentile,
                        EmitMode.PERIODIC);

        long clock = 0;
        for (int i = 0; i < DEFAULT_MAX_CAPACITY * 2; i++) {
            clock++;
            final int highDelay = i % 1000 == 0 ? Integer.MIN_VALUE : 0;
            g.onEvent(null, clock + highDelay, null);
        }
        // 10 outliers but only 5 are still in buffers
        assertThat(g.bucketCounts[g.bucketCounts.length - 1]).isEqualTo(5);
        assertThat(g.percentileDelay).isEqualTo(57);
    }

    @Test
    void testFilterCommonIssues() {
        final HistogramWatermarkGenerator g = new HistogramWatermarkGenerator();

        g.onEvent(null, Long.MAX_VALUE, null);
        // Filter Long.MAX_VALUE
        assertThat(g.maxTimestamp).isEqualTo(Long.MIN_VALUE);

        g.onEvent(null, 42, null);
        // Filter Long.MIN/MAX_VALUE
        g.onEvent(null, Long.MIN_VALUE, null);
        g.onEvent(null, Long.MAX_VALUE, null);
        assertThat(g.maxTimestamp).isEqualTo(42);
    }

    @Test
    void testWatermarkEmission() {
        final int capacity = 5;
        final int bucketTarget = 10;
        final int minDelay = 50;
        final int maxDelay = 1000;
        final double percentile = 1.0;
        final HistogramWatermarkGenerator g =
                new HistogramWatermarkGenerator(
                        capacity, bucketTarget, minDelay, maxDelay, percentile, EmitMode.PERIODIC);

        // Completely fills the buffer
        g.onEvent(null, 1, null);
        g.onEvent(null, 2, null);
        g.onEvent(null, 3, null);
        g.onEvent(null, 4, null);
        g.onEvent(null, 5, null);
        g.onEvent(null, 6, null);

        final TestingWatermarkOutput testingWatermarkOutput = new TestingWatermarkOutput();

        g.onPeriodicEmit(testingWatermarkOutput);

        assertThat(g.maxTimestamp).isEqualTo(6);
        assertThat(g.percentileDelay).isEqualTo(63);
        // maxTimestamp - percentileDelay
        assertThat(testingWatermarkOutput.value).isEqualTo(-57);
    }

    @Test
    void testMinimumWatermark() {
        final HistogramWatermarkGenerator g = new HistogramWatermarkGenerator();

        g.onEvent(null, Long.MIN_VALUE + 1, null);
        g.onEvent(null, Long.MIN_VALUE + 2, null);

        final TestingWatermarkOutput testingWatermarkOutput = new TestingWatermarkOutput();
        g.onPeriodicEmit(testingWatermarkOutput);
        // No overflow
        assertThat(testingWatermarkOutput.value).isEqualTo(Long.MIN_VALUE);
    }

    // @Test
    void testPerformance() {
        final HistogramWatermarkGenerator g = new HistogramWatermarkGenerator();

        final ThreadLocalRandom r = ThreadLocalRandom.current();
        int ts = 1;

        // Warm up
        for (int i = 0; i < 10000; i++) {
            ts++;
            g.onEvent(null, ts + r.nextInt(-2000, +2000), null);
        }

        // Measure
        long sum = 0;
        long count = 0;
        for (int i = 0; i < 100000000; i++) {
            ts++;
            final long tsWithDelay = ts + r.nextInt(-2000, +2000);

            final long start = System.nanoTime();
            g.onEvent(null, tsWithDelay, null);
            final long measured = System.nanoTime() - start;

            sum += measured;
            count += 1;
        }

        System.out.println("Average: " + sum / count);
        System.out.println("Percentile delay: " + g.percentileDelay);
    }

    // --------------------------------------------------------------------------------------------

    private static class TestingWatermarkOutput implements WatermarkOutput {

        long value = Long.MIN_VALUE;

        @Override
        public void emitWatermark(Watermark watermark) {
            value = watermark.getTimestamp();
        }

        @Override
        public void markIdle() {
            // Nothing to do
        }

        @Override
        public void markActive() {
            // Nothing to do
        }
    }
}
