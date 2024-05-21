/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;

import io.confluent.flink.udf.adapter.codegen.ScalarFunctionCallAdapter;

/**
 * Adapter layer that binds a {@link ScalarFunctionCallAdapter} to an instance of {@link
 * ScalarFunction} for invocation.
 */
public class ScalarFunctionInstanceCallAdapter {

    /** Instance-agnostic call adapter. */
    private final RichMapFunction<RowData, Object> callAdapter;

    private final Configuration tableConfiguration;

    public ScalarFunctionInstanceCallAdapter(
            RichMapFunction<RowData, Object> callAdapter, Configuration tableConfiguration) {
        this.callAdapter = callAdapter;
        this.tableConfiguration = tableConfiguration;
    }

    public Object call(Object[] args) throws Throwable {
        GenericRowData genericRowData = GenericRowData.of(args);
        return callAdapter.map(genericRowData);
    }

    public void open() throws Exception {
        callAdapter.setRuntimeContext(new UdfRuntimeContext());
        callAdapter.open(tableConfiguration);
    }

    public void close() throws Exception {
        callAdapter.close();
    }
}
