/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.examples;

import org.apache.flink.table.functions.ScalarFunction;

/** Sum with multiple overloaded functions. */
public class OverloadedSumScalarFunction extends ScalarFunction {

    public Integer eval(Integer x, Integer y) {
        return x + y;
    }

    public Integer eval(Integer x, Integer y, Integer z) {
        return x + y + z;
    }

    public Integer eval(Integer x, Integer y, Integer z, Integer a) {
        return x + y + z + a;
    }
}
