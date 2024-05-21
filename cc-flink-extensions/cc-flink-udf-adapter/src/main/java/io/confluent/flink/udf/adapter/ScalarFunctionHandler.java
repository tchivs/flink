/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.ExceptionUtils;

import io.confluent.flink.udf.adapter.serde.SerDeScalarFunctionCallAdapter;
import io.confluent.function.runtime.core.Context;
import io.confluent.function.runtime.core.RequestHandler;
import io.confluent.function.runtime.core.RequestInvocationException;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic proxy that forwards calls to the {@link ScalarFunction} specified in the payload as
 * "functionClassName".
 */
public class ScalarFunctionHandler implements RequestHandler {

    private static final Logger logger = Logger.getLogger(ScalarFunctionHandler.class.getName());
    public static final int ADAPTER_INVOCATION_EXCEPTION = 101;

    private SerDeScalarFunctionCallAdapter callAdapter;

    public ScalarFunctionHandler() {
        callAdapter = null;
    }

    @Override
    public void open(byte[] payload, Context ctx) {
        String instanceId = UUID.randomUUID().toString();
        try {
            this.callAdapter =
                    SerDeScalarFunctionCallAdapter.create(
                            instanceId, payload, Thread.currentThread().getContextClassLoader());
            this.callAdapter.open();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Open failure: {0}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] handleRequest(byte[] payload, Context context) throws RequestInvocationException {
        try {
            return callAdapter.call(payload);
        } catch (Throwable t) {
            final String errorMsg =
                    String.format(
                            "Failure in handleRequest: %s", ExceptionUtils.stringifyException(t));
            logger.log(Level.WARNING, errorMsg);
            throw new RequestInvocationException(ADAPTER_INVOCATION_EXCEPTION, errorMsg);
        }
    }

    @Override
    public void close(byte[] payload, Context ctx) {
        try {
            this.callAdapter.close();
        } catch (Exception e) {
            logger.log(Level.WARNING, "Close failure: {0}", e);
        }
        this.callAdapter = null;
    }

    @VisibleForTesting
    SerDeScalarFunctionCallAdapter getCallAdapter() {
        return callAdapter;
    }
}
