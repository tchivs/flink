/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import io.confluent.flink.udf.adapter.api.AdapterOptions;
import io.confluent.flink.udf.adapter.api.RemoteUdfSerialization;
import io.confluent.flink.udf.adapter.examples.HelloScalarFunction;
import io.confluent.flink.udf.adapter.examples.SumScalarFunction;
import io.confluent.function.runtime.core.RequestInvocationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.confluent.flink.udf.adapter.TestUtil.DUMMY_CONTEXT;
import static io.confluent.flink.udf.adapter.TestUtil.createSerializedOpenPayload;
import static io.confluent.flink.udf.adapter.TestUtil.createSerializers;
import static io.confluent.flink.udf.adapter.TestUtil.testInvoke;
import static io.confluent.flink.udf.adapter.TestUtil.testInvokeBatch;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ScalarFunctionHandler}. */
public class ScalarFunctionHandlerTest {

    final Configuration configuration = new Configuration();

    final ScalarFunctionHandler functionHandler = new ScalarFunctionHandler();

    @AfterEach
    public void tearDown() {
        functionHandler.close(new byte[0], DUMMY_CONTEXT);
    }

    /** Test basic invocation of generic functions. */
    @Test
    public void testInvokeIdentityFunction() throws Throwable {
        functionHandler.open(
                createSerializedOpenPayload(
                        "testOrg",
                        "testEnv",
                        "pluginUUID",
                        "pluginVersionUUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(new VarCharType()),
                        IdentityScalarFunction.class.getName(),
                        true,
                        configuration),
                DUMMY_CONTEXT);
        RemoteUdfSerialization serializers = createSerializers(functionHandler);
        final int invocations = 10;
        for (int i = 0; i < invocations; i++) {
            StringData argument = StringData.fromString(String.valueOf(i));
            Assertions.assertEquals(
                    argument, testInvoke(functionHandler, new Object[] {argument}, serializers));
        }
        functionHandler.close(new byte[0], DUMMY_CONTEXT);
    }

    /** Test basic invocation of generic functions. */
    @Test
    public void testConcatFunction() throws Throwable {
        functionHandler.open(
                createSerializedOpenPayload(
                        "testOrg",
                        "testEnv",
                        "pluginUUID",
                        "pluginVersionUUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(
                                new VarCharType(Integer.MAX_VALUE),
                                new VarCharType(Integer.MAX_VALUE)),
                        ConcatFunction.class.getName(),
                        true,
                        configuration),
                DUMMY_CONTEXT);
        RemoteUdfSerialization serializers = createSerializers(functionHandler);
        final int invocations = 10;
        for (int i = 0; i < invocations; i++) {
            StringData argument = StringData.fromString(String.valueOf(i));
            StringData result = StringData.fromString(String.valueOf(i) + i);
            Assertions.assertEquals(
                    result,
                    testInvoke(functionHandler, new Object[] {argument, argument}, serializers));
        }
        functionHandler.close(new byte[0], DUMMY_CONTEXT);
    }

    /** Test basic invocation of generic functions. */
    @Test
    public void testInvokeSumFunction() throws Throwable {
        functionHandler.open(
                createSerializedOpenPayload(
                        "testOrg",
                        "testEnv",
                        "pluginUUID",
                        "pluginVersionUUID",
                        new IntType(),
                        Arrays.asList(new IntType(), new IntType()),
                        SumScalarFunction.class.getName(),
                        true,
                        configuration),
                DUMMY_CONTEXT);
        RemoteUdfSerialization serializers = createSerializers(functionHandler);
        final int invocations = 10;
        for (int i = 0; i < invocations; i++) {
            int sum = i + i;
            Assertions.assertEquals(
                    sum, testInvoke(functionHandler, new Object[] {i, i}, serializers));
        }
        functionHandler.close(new byte[0], DUMMY_CONTEXT);
    }

    /** Test basic invocation of generic functions. */
    @Test
    public void testInvokeHelloFunction() throws Throwable {
        functionHandler.open(
                createSerializedOpenPayload(
                        "testOrg",
                        "testEnv",
                        "pluginUUID",
                        "pluginVersionUUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(new VarCharType()),
                        HelloScalarFunction.class.getName(),
                        true,
                        configuration),
                DUMMY_CONTEXT);
        RemoteUdfSerialization serializers = createSerializers(functionHandler);
        Assertions.assertTrue(
                testInvoke(
                                functionHandler,
                                new Object[] {StringData.fromString("Stefan")},
                                serializers)
                        .toString()
                        .startsWith("Hello, Stefan!"));
        functionHandler.close(new byte[0], DUMMY_CONTEXT);
    }

    /** Tests that we fail if no matching function is found in the specified class. */
    @Test
    public void testNoMatchingFunction() {
        assertThatThrownBy(
                        () -> {
                            final ScalarFunctionHandler functionHandler =
                                    new ScalarFunctionHandler();
                            functionHandler.open(
                                    createSerializedOpenPayload(
                                            "testOrg",
                                            "testEnv",
                                            "pluginUUID",
                                            "pluginVersionUUID",
                                            new IntType(),
                                            Arrays.asList(new IntType(false)),
                                            NoMatchingScalarFunction.class.getName(),
                                            true,
                                            configuration),
                                    DUMMY_CONTEXT);
                        })
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Mismatch of expected output data type");
    }

    @Test
    public void testTimeoutHandler() throws Throwable {
        configuration.set(AdapterOptions.ADAPTER_PARALLELISM, 1);
        configuration.set(AdapterOptions.ADAPTER_HANDLER_WAIT_TIMEOUT, Duration.ofMillis(200));
        functionHandler.open(
                createSerializedOpenPayload(
                        "testOrg",
                        "testEnv",
                        "pluginUUID",
                        "pluginVersionUUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(new VarCharType()),
                        SleepingHelloScalarFunction.class.getName(),
                        true,
                        configuration),
                DUMMY_CONTEXT);
        RemoteUdfSerialization serializers = createSerializers(functionHandler);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        SleepingHelloScalarFunction.running.set(false);
        executor.execute(
                () -> {
                    try {
                        Assertions.assertTrue(
                                testInvoke(
                                                functionHandler,
                                                new Object[] {StringData.fromString("Stefan")},
                                                serializers)
                                        .toString()
                                        .startsWith("Hello, Stefan!"));
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                });

        while (!SleepingHelloScalarFunction.running.get()) {
            Thread.sleep(10);
        }

        assertThatThrownBy(
                        () -> {
                            testInvoke(
                                            functionHandler,
                                            new Object[] {StringData.fromString("Stefan")},
                                            serializers)
                                    .toString()
                                    .startsWith("Hello, Stefan!");
                        })
                .isInstanceOf(RequestInvocationException.class)
                .hasMessageContaining("Can't get an adapter in time");

        functionHandler.close(new byte[0], DUMMY_CONTEXT);
        executor.shutdownNow();
    }

    @Test
    public void testInvokeConcatFunctionWithNull() throws Throwable {
        functionHandler.open(
                createSerializedOpenPayload(
                        "testOrg",
                        "testEnv",
                        "pluginUUID",
                        "pluginVersionUUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(new VarCharType(), new VarCharType()),
                        ConcatFunction.class.getName(),
                        true,
                        configuration),
                DUMMY_CONTEXT);
        RemoteUdfSerialization serializers = createSerializers(functionHandler);
        assertThat(
                        testInvoke(
                                        functionHandler,
                                        new Object[] {StringData.fromString("blah "), null},
                                        serializers)
                                .toString())
                .isEqualTo("blah null");
        functionHandler.close(new byte[0], DUMMY_CONTEXT);
    }

    @Test
    public void testInvokeConcatFunctionWithBatch() throws Throwable {
        configuration.set(AdapterOptions.ADAPTER_HANDLER_BATCH_ENABLED, true);
        functionHandler.open(
                createSerializedOpenPayload(
                        "testOrg",
                        "testEnv",
                        "pluginUUID",
                        "pluginVersionUUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(new VarCharType(), new VarCharType()),
                        ConcatFunction.class.getName(),
                        true,
                        configuration),
                DUMMY_CONTEXT);
        RemoteUdfSerialization serializers = createSerializers(functionHandler);
        assertThat(
                        testInvokeBatch(
                                functionHandler,
                                Arrays.asList(
                                        new Object[] {
                                            StringData.fromString("a"), StringData.fromString("b")
                                        },
                                        new Object[] {
                                            StringData.fromString("c"), StringData.fromString("d")
                                        },
                                        new Object[] {
                                            StringData.fromString("e"), StringData.fromString("f")
                                        }),
                                serializers))
                .containsExactly(
                        StringData.fromString("ab"),
                        StringData.fromString("cd"),
                        StringData.fromString("ef"));
        functionHandler.close(new byte[0], DUMMY_CONTEXT);
    }

    /** Simple ScalarFunction with identity behavior for String and Integer types. */
    public static class IdentityScalarFunction extends ScalarFunction {
        public String eval(String argument) {
            return argument;
        }

        public Integer eval(Integer argument) {
            return argument;
        }
    }

    /** Simple ScalarFunction returning the String representation of an Integer. */
    public static class NoMatchingScalarFunction extends ScalarFunction {
        public String eval(int argument) {
            return String.valueOf(argument);
        }
    }

    /** Simple ScalarFunction concatenating strings. */
    public static class ConcatFunction extends ScalarFunction {
        public String eval(String a, String b) {
            return a + b;
        }
    }

    /** Sleeps for 5s during run. */
    public static class SleepingHelloScalarFunction extends ScalarFunction {
        static AtomicBoolean running = new AtomicBoolean(false);

        public String eval(String name) throws InterruptedException {
            running.set(true);
            Thread.sleep(5000);
            return "Hello, " + name.toString() + "!         --- " + new java.util.Date();
        }
    }
}
