/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import io.confluent.flink.udf.adapter.codegen.ScalarFunctionCallAdapter;

/**
 * Adapter layer that binds a {@link ScalarFunctionCallAdapter} to an instance of {@link
 * ScalarFunction} for invocation.
 */
public class ScalarFunctionInstanceCallAdapter {

    /** Instance to invoke. */
    private final ScalarFunction instance;

    /** Instance-agnostic call adapter. */
    private final ScalarFunctionCallAdapter callAdapter;

    public ScalarFunctionInstanceCallAdapter(
            ScalarFunction instance, ScalarFunctionCallAdapter callAdapter) {
        this.instance = instance;
        this.callAdapter = callAdapter;
    }

    public Object call(Object[] args) throws Throwable {
        return callAdapter.call(instance, args);
    }

    public void open() throws Exception {
        instance.open(new FunctionContext(null));
    }

    public void close() throws Exception {
        instance.close();
    }
}
