/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ai;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.FlinkRuntimeException;

/** Class implementing aiSecret function. */
public class AISecret extends ScalarFunction {

    public static final String NAME = "SECRET";

    public static String eval(String... args) {
        String key = System.getenv("OPENAI_API_KEY");
        if (key == null) {
            throw new FlinkRuntimeException("Must set environment variable OPENAI_API_KEY");
        }
        return key;
    }
}
