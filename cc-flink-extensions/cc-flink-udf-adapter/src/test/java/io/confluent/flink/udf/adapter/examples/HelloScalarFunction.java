/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.examples;

import org.apache.flink.table.functions.ScalarFunction;

/** A simple scalar function that returns a greeting message. */
public class HelloScalarFunction extends ScalarFunction {
    public String eval(String name) {
        return "Hello, " + name.toString() + "!         --- " + new java.util.Date();
    }
}
