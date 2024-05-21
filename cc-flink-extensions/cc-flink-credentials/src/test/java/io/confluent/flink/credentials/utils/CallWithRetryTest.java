/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.credentials.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CallWithRetry}. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class CallWithRetryTest {

    private final MockSleeper sleeper = new MockSleeper();
    private CallWithRetry callWithRetry;

    @BeforeEach
    public void setUp() throws IOException {
        callWithRetry = new CallWithRetry(3, 500, sleeper);
    }

    @Test
    public void testSuccess() {
        String result =
                callWithRetry.call(
                        () -> {
                            return "foo";
                        });
        assertThat(result).isEqualTo("foo");
        assertThat(sleeper.getSleeps()).containsExactly();
    }

    @Test
    public void testSuccessAfterFailures() {
        AtomicInteger failures = new AtomicInteger(0);
        String result =
                callWithRetry.call(
                        () -> {
                            if (failures.getAndIncrement() < 2) {
                                throw new RuntimeException();
                            }
                            return "foo";
                        });
        assertThat(result).isEqualTo("foo");
        assertThat(sleeper.getSleeps()).containsExactly(500L, 750L);
    }

    @Test
    public void testFailure() {
        AtomicInteger failures = new AtomicInteger(0);
        assertThatThrownBy(
                        () -> {
                            callWithRetry.call(
                                    () -> {
                                        if (failures.getAndIncrement() < 3) {
                                            throw new RuntimeException("Failure!!!");
                                        }
                                        return "foo";
                                    });
                        })
                .hasMessageContaining("Failure!!!");
        assertThat(sleeper.getSleeps()).containsExactly(500L, 750L);
    }

    @Test
    public void testFailureManyRetries() {
        callWithRetry = new CallWithRetry(20, 1000, sleeper);
        AtomicInteger failures = new AtomicInteger(0);
        assertThatThrownBy(
                        () -> {
                            callWithRetry.call(
                                    () -> {
                                        if (failures.getAndIncrement() < 21) {
                                            throw new RuntimeException("Failure!!!");
                                        }
                                        return "foo";
                                    });
                        })
                .hasMessageContaining("Failure!!!");
        assertThat(sleeper.getSleeps())
                .containsExactly(
                        1000L, 1500L, 2250L, 3375L, 5062L, 7593L, 11389L, 17083L, 25624L, 38436L,
                        57654L, 86481L, 129721L, 194581L, 291871L, 437806L, 656709L, 985063L,
                        1477594L);
    }
}
