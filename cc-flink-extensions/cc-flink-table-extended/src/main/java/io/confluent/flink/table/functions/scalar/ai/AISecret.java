/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.functions.scalar.ai;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.runtime.functions.scalar.BuiltInScalarFunction;
import org.apache.flink.util.FlinkRuntimeException;

/** Class implementing aiSecret function. */
public class AISecret extends BuiltInScalarFunction {

    public AISecret(SpecializedContext context) {
        super(AIFunctionsModule.SECRET, context);
    }

    public static StringData eval(StringData... args) {
        String key = System.getenv("OPENAI_API_KEY");
        if (key == null) {
            throw new FlinkRuntimeException("Must set environment variable OPENAI_API_KEY");
        }
        return StringData.fromString(key);
    }
}
