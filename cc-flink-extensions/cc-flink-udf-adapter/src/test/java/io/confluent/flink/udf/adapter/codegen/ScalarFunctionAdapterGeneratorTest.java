/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.codegen;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;

import io.confluent.flink.udf.adapter.ScalarFunctionInstanceCallAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class ScalarFunctionAdapterGeneratorTest {

    @Test
    public void testEval_0() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Collections.emptyList(),
                        getClass().getClassLoader());
        Assertions.assertEquals(instance.eval(), generated.call(new Object[0]));
    }

    @Test
    public void testEval_1() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Collections.singletonList(long.class),
                        getClass().getClassLoader());
        Assertions.assertEquals(instance.eval(42L), generated.call(new Object[] {42L}));
        Assertions.assertThrowsExactly(
                NullPointerException.class, () -> generated.call(new Object[] {null}));
    }

    @Test
    public void testEval_2() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Collections.singletonList(Integer.class),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval(Integer.valueOf(42)), generated.call(new Object[] {42}));
        Assertions.assertEquals(instance.eval((Integer) null), generated.call(new Object[] {null}));

        ScalarFunctionInstanceCallAdapter generated2 =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Collections.singletonList(int.class),
                        getClass().getClassLoader());
        String expected = instance.eval(42);
        Assertions.assertTrue(expected.startsWith("1"));
        Assertions.assertEquals(expected, generated2.call(new Object[] {42}));
        Assertions.assertThrowsExactly(
                NullPointerException.class, () -> generated2.call(new Object[] {null}));
    }

    @Test
    public void testEval_3() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Arrays.asList(Integer.class, int.class),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval(Integer.valueOf(42), 23), generated.call(new Object[] {42, 23}));
        Assertions.assertEquals(instance.eval(null, 23), generated.call(new Object[] {null, 23}));
    }

    @Test
    public void testEval_4() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Arrays.asList(int.class, int.class),
                        getClass().getClassLoader());
        Assertions.assertEquals(instance.eval(42, 23), generated.call(new Object[] {42, 23}));
    }

    @Test
    public void testEval_5() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Arrays.asList(Integer.class, Integer.class),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval(Integer.valueOf(42), Integer.valueOf(23)),
                generated.call(new Object[] {42, 23}));
        Assertions.assertEquals(
                instance.eval(null, null), generated.call(new Object[] {null, null}));
    }

    @Test
    public void testEval_6() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generatedArray =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Arrays.asList(String[].class),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval((String[]) null), generatedArray.call(new Object[] {null}));
        Assertions.assertEquals(
                instance.eval(new String[] {}),
                generatedArray.call(new Object[] {new String[] {}}));
        Assertions.assertEquals(
                instance.eval(new String[] {"A"}),
                generatedArray.call(new Object[] {new String[] {"A"}}));
        Assertions.assertEquals(
                instance.eval(new String[] {"A", "B"}),
                generatedArray.call(new Object[] {new String[] {"A", "B"}}));
        Assertions.assertEquals(
                instance.eval(new String[] {"A", "B", "C"}),
                generatedArray.call(new Object[] {new String[] {"A", "B", "C"}}));
        Assertions.assertEquals(
                instance.eval(new String[] {"A", "B", "C", "D"}),
                generatedArray.call(new Object[] {new String[] {"A", "B", "C", "D"}}));
        Assertions.assertEquals(
                instance.eval(new String[] {null}),
                generatedArray.call(new Object[] {new String[] {null}}));
        Assertions.assertEquals(
                instance.eval(new String[] {null, null}),
                generatedArray.call(new Object[] {new String[] {null, null}}));
        Assertions.assertEquals(
                instance.eval(new String[] {null, null, null}),
                generatedArray.call(new Object[] {new String[] {null, null, null}}));
        Assertions.assertEquals(
                instance.eval(new String[] {null, null, null, null}),
                generatedArray.call(new Object[] {new String[] {null, null, null, null}}));
        Assertions.assertEquals(
                instance.eval(new String[] {null, "B", "C"}),
                generatedArray.call(new Object[] {new String[] {null, "B", "C"}}));
        Assertions.assertEquals(
                instance.eval(new String[] {"A", "B", "C", null}),
                generatedArray.call(new Object[] {new String[] {"A", "B", "C", null}}));

        String expected = (String) generatedArray.call(new Object[] {new String[] {"A", "B"}});
        Assertions.assertTrue(expected.startsWith("6"));

        ScalarFunctionInstanceCallAdapter generatedVarargs =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Arrays.asList(String.class, String.class, String.class),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval("A", "B", "C"), generatedVarargs.call(new Object[] {"A", "B", "C"}));
        Assertions.assertEquals(
                instance.eval(null, "B", "C"),
                generatedVarargs.call(new Object[] {null, "B", "C"}));
        Assertions.assertEquals(
                instance.eval(null, null, (String) null),
                generatedVarargs.call(new Object[] {null, null, null}));
    }

    @Test
    public void testEval_7() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        String.class,
                        Arrays.asList(String.class, String.class, String[].class),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval(null, null, (String[]) null),
                generated.call(new Object[] {null, null, null}));
        Assertions.assertEquals(
                instance.eval(null, null, new String[] {}),
                generated.call(new Object[] {null, null, new String[] {}}));
        Assertions.assertEquals(
                instance.eval(null, null, new String[] {null}),
                generated.call(new Object[] {null, null, new String[] {null}}));
        Assertions.assertEquals(
                instance.eval("A", null, new String[] {}),
                generated.call(new Object[] {"A", null, new String[] {}}));
        Assertions.assertEquals(
                instance.eval("A", "B", new String[] {}),
                generated.call(new Object[] {"A", "B", new String[] {}}));
        Assertions.assertEquals(
                instance.eval("A", "B", new String[] {"C"}),
                generated.call(new Object[] {"A", "B", new String[] {"C"}}));
        Assertions.assertEquals(
                instance.eval(null, "B", new String[] {"C"}),
                generated.call(new Object[] {null, "B", new String[] {"C"}}));
        Assertions.assertEquals(
                instance.eval("A", "B", null), generated.call(new Object[] {"A", "B", null}));
        Assertions.assertEquals(
                instance.eval("A", "B", new String[] {"C", "D"}),
                generated.call(new Object[] {"A", "B", new String[] {"C", "D"}}));
        Assertions.assertEquals(
                instance.eval("A", "B", new String[] {null, "D"}),
                generated.call(new Object[] {"A", "B", new String[] {null, "D"}}));
    }

    public static class TestFun extends ScalarFunction {

        public String eval() {
            return "0";
        }

        public String eval(long x) {
            return "1 " + x;
        }

        public String eval(Integer x) {
            return "2 " + x;
        }

        public String eval(Integer x, int y) {
            return "3 " + x + " " + y;
        }

        public String eval(int x, int y) {
            return "4 " + x + " " + y;
        }

        public String eval(Integer x, Integer y) {
            return "5 " + x + " " + y;
        }

        public String eval(String... x) {
            return "6 " + Arrays.toString(x);
        }

        public String eval(String x, String y, String[] z) {
            return "7 " + x + " " + y + " " + Arrays.toString(z);
        }
    }
}
