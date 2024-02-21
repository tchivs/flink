/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.codegen;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;

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
                        Collections.emptyList(),
                        DataTypes.STRING(),
                        getClass().getClassLoader());
        Assertions.assertEquals(instance.eval(), generated.call(new Object[0]).toString());
    }

    @Test
    public void testEval_1() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        Collections.singletonList(new AtomicDataType(new BigIntType(false))),
                        DataTypes.STRING(),
                        getClass().getClassLoader());
        Assertions.assertEquals(instance.eval(42L), generated.call(new Object[] {42L}).toString());
        // Generated code defaults longs to -1 if they are given a null rather than throw an NPE
        Assertions.assertEquals("1 -1", generated.call(new Object[] {null}).toString());
    }

    @Test
    public void testEval_2() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        Collections.singletonList(DataTypes.INT()),
                        DataTypes.STRING(),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval(Integer.valueOf(42)), generated.call(new Object[] {42}).toString());
        Assertions.assertEquals(
                instance.eval((Integer) null), generated.call(new Object[] {null}).toString());

        ScalarFunctionInstanceCallAdapter generated2 =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        Collections.singletonList(new AtomicDataType(new IntType(false))),
                        DataTypes.STRING(),
                        getClass().getClassLoader());
        // Flink code gen casts to ints to Integer, even if it cannot be null.
        // So we tell it to invoke the Integer version here.
        String expected = instance.eval((Integer) 42);
        Assertions.assertTrue(expected.startsWith("2"));
        Assertions.assertEquals(expected, generated2.call(new Object[] {42}).toString());
        // Because there's a nullable version, it just prints null
        Assertions.assertEquals("2 null", generated2.call(new Object[] {null}).toString());
    }

    @Test
    public void testEval_3() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        Arrays.asList(new AtomicDataType(new IntType(false)), DataTypes.INT()),
                        DataTypes.STRING(),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval(Integer.valueOf(42), Integer.valueOf(23)),
                generated.call(new Object[] {42, 23}).toString());
        // Flink code gen casts to ints to Integer, even if it cannot be null.
        // So we tell it to invoke the Integer version here.
        Assertions.assertEquals(
                instance.eval((Integer) null, (Integer) 23),
                generated.call(new Object[] {null, 23}).toString());
    }

    @Test
    public void testEval_4() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        Arrays.asList(
                                new AtomicDataType(new IntType(false)),
                                new AtomicDataType(new IntType(false))),
                        DataTypes.STRING(),
                        getClass().getClassLoader());
        // Flink code gen casts to ints to Integer, even if it cannot be null.
        // So we tell it to invoke the Integer version here.
        Assertions.assertEquals(
                instance.eval((Integer) 42, (Integer) 23),
                generated.call(new Object[] {42, 23}).toString());
    }

    @Test
    public void testEval_5() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        Arrays.asList(DataTypes.INT(), DataTypes.INT()),
                        DataTypes.STRING(),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval(Integer.valueOf(42), Integer.valueOf(23)),
                generated.call(new Object[] {42, 23}).toString());
        Assertions.assertEquals(
                instance.eval(null, null), generated.call(new Object[] {null, null}).toString());
    }

    @Test
    public void testEval_7() throws Throwable {
        TestFun instance = new TestFun();
        ScalarFunctionInstanceCallAdapter generated =
                ScalarFunctionAdapterGenerator.generate(
                        "",
                        instance,
                        Arrays.asList(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.STRING(),
                        getClass().getClassLoader());
        Assertions.assertEquals(
                instance.eval(null, null, (String[]) null),
                generated.call(new Object[] {null, null, null}).toString());
        Assertions.assertEquals(
                instance.eval(null, null, new String[] {}),
                generated
                        .call(new Object[] {null, null, new GenericArrayData(new StringData[0])})
                        .toString());
        Assertions.assertEquals(
                instance.eval(null, null, new String[] {null}),
                generated
                        .call(
                                new Object[] {
                                    null, null, new GenericArrayData(new StringData[] {null})
                                })
                        .toString());
        Assertions.assertEquals(
                instance.eval("A", null, new String[] {}),
                generated
                        .call(
                                new Object[] {
                                    StringData.fromString("A"),
                                    null,
                                    new GenericArrayData(new StringData[] {})
                                })
                        .toString());
        Assertions.assertEquals(
                instance.eval("A", "B", new String[] {}),
                generated
                        .call(
                                new Object[] {
                                    StringData.fromString("A"),
                                    StringData.fromString("B"),
                                    new GenericArrayData(new StringData[] {})
                                })
                        .toString());
        Assertions.assertEquals(
                instance.eval("A", "B", new String[] {"C"}),
                generated
                        .call(
                                new Object[] {
                                    StringData.fromString("A"),
                                    StringData.fromString("B"),
                                    new GenericArrayData(
                                            new StringData[] {StringData.fromString("C")})
                                })
                        .toString());
        Assertions.assertEquals(
                instance.eval(null, "B", new String[] {"C"}),
                generated
                        .call(
                                new Object[] {
                                    null,
                                    StringData.fromString("B"),
                                    new GenericArrayData(
                                            new StringData[] {StringData.fromString("C")})
                                })
                        .toString());
        Assertions.assertEquals(
                instance.eval("A", "B", null),
                generated
                        .call(
                                new Object[] {
                                    StringData.fromString("A"), StringData.fromString("B"), null
                                })
                        .toString());
        Assertions.assertEquals(
                instance.eval("A", "B", new String[] {"C", "D"}),
                generated
                        .call(
                                new Object[] {
                                    StringData.fromString("A"),
                                    StringData.fromString("B"),
                                    new GenericArrayData(
                                            new StringData[] {
                                                StringData.fromString("C"),
                                                StringData.fromString("D")
                                            })
                                })
                        .toString());
        Assertions.assertEquals(
                instance.eval("A", "B", new String[] {null, "D"}),
                generated
                        .call(
                                new Object[] {
                                    StringData.fromString("A"),
                                    StringData.fromString("B"),
                                    new GenericArrayData(
                                            new StringData[] {null, StringData.fromString("D")})
                                })
                        .toString());
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

        public String eval(String x, String y, String[] z) {
            return "7 " + x + " " + y + " " + Arrays.toString(z);
        }
    }
}
