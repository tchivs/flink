/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.examples;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ScalarFunction;

/** A simple scalar function that returns a greeting message. */
public class HelloScalarFunction extends ScalarFunction {
    public StringData eval(StringData name) {
        return StringData.fromString(
                "Hello, " + name.toString() + "!         --- " + new java.util.Date());
    }
}
