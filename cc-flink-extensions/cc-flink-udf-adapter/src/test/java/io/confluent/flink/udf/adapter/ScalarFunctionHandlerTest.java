/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.VarCharType;

import io.confluent.flink.table.modules.remoteudf.RemoteUdfSerialization;
import io.confluent.flink.udf.adapter.examples.HelloScalarFunction;
import io.confluent.flink.udf.adapter.examples.SumScalarFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static io.confluent.flink.udf.adapter.TestUtil.DUMMY_CONTEXT;
import static io.confluent.flink.udf.adapter.TestUtil.createSerializedOpenPayload;
import static io.confluent.flink.udf.adapter.TestUtil.createSerializers;
import static io.confluent.flink.udf.adapter.TestUtil.testInvoke;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link ScalarFunctionHandler}. */
public class ScalarFunctionHandlerTest {

    /** Test basic invocation of generic functions. */
    @Test
    public void testInvokeIdentityFunction() throws Throwable {
        final ScalarFunctionHandler functionHandler = new ScalarFunctionHandler();
        functionHandler.open(
                createSerializedOpenPayload(
                        "UUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(new VarCharType()),
                        IdentityScalarFunction.class.getName()),
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
        final ScalarFunctionHandler functionHandler = new ScalarFunctionHandler();
        functionHandler.open(
                createSerializedOpenPayload(
                        "UUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(
                                new VarCharType(Integer.MAX_VALUE),
                                new VarCharType(Integer.MAX_VALUE)),
                        ConcatFunction.class.getName()),
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
        final ScalarFunctionHandler functionHandler = new ScalarFunctionHandler();
        functionHandler.open(
                createSerializedOpenPayload(
                        "UUID",
                        new IntType(),
                        Arrays.asList(new IntType(), new IntType()),
                        SumScalarFunction.class.getName()),
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
        final ScalarFunctionHandler functionHandler = new ScalarFunctionHandler();
        functionHandler.open(
                createSerializedOpenPayload(
                        "UUID",
                        new VarCharType(Integer.MAX_VALUE),
                        Arrays.asList(new VarCharType()),
                        HelloScalarFunction.class.getName()),
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
        Assertions.assertTrue(
                assertThrows(
                                RuntimeException.class,
                                () -> {
                                    final ScalarFunctionHandler functionHandler =
                                            new ScalarFunctionHandler();
                                    functionHandler.open(
                                            createSerializedOpenPayload(
                                                    "UUID",
                                                    new IntType(),
                                                    Arrays.asList(new IntType(false)),
                                                    NoMatchingScalarFunction.class.getName()),
                                            DUMMY_CONTEXT);
                                })
                        .getMessage()
                        .contains("Mismatch of expected output data type"));
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
}
