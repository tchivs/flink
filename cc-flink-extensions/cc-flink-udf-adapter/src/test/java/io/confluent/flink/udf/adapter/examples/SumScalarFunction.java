/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.examples;

import org.apache.flink.table.functions.ScalarFunction;

/** A simple scalar function that returns the sum of two integers. */
public class SumScalarFunction extends ScalarFunction {

    public Integer eval(Integer x, Integer y) {
        return x + y;
    }
}
