/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.ExceptionUtils;

import io.confluent.flink.udf.adapter.api.AdapterOptions;
import io.confluent.flink.udf.adapter.api.OpenPayload;
import io.confluent.flink.udf.adapter.serde.SerDeScalarFunctionCallAdapter;
import io.confluent.function.runtime.core.Context;
import io.confluent.function.runtime.core.RequestHandler;
import io.confluent.function.runtime.core.RequestInvocationException;

import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generic proxy that forwards calls to the {@link ScalarFunction} specified in the payload as
 * "functionClassName".
 */
public class ScalarFunctionHandler implements RequestHandler {

    private static final Logger logger = Logger.getLogger(ScalarFunctionHandler.class.getName());
    public static final int ADAPTER_INVOCATION_EXCEPTION = 101;
    private LinkedBlockingQueue<SerDeScalarFunctionCallAdapter> callAdapters =
            new LinkedBlockingQueue<>();
    private long waitTimeoutMs;

    public ScalarFunctionHandler() {}

    @Override
    public void open(byte[] payload, Context ctx) {
        try {
            OpenPayload open =
                    OpenPayload.open(payload, Thread.currentThread().getContextClassLoader());
            Configuration configuration = open.getConfiguration();
            waitTimeoutMs =
                    configuration.get(AdapterOptions.ADAPTER_HANDLER_WAIT_TIMEOUT).toMillis();
            int parallelism = configuration.get(AdapterOptions.ADAPTER_PARALLELISM);
            for (int i = 0; i < parallelism; i++) {
                String instanceId = UUID.randomUUID().toString();
                SerDeScalarFunctionCallAdapter callAdapter =
                        SerDeScalarFunctionCallAdapter.create(
                                instanceId,
                                payload,
                                Thread.currentThread().getContextClassLoader());
                callAdapter.open();
                callAdapters.add(callAdapter);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Open failure: {0}", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] handleRequest(byte[] payload, Context context) throws RequestInvocationException {
        SerDeScalarFunctionCallAdapter callAdapter = null;
        try {
            callAdapter = callAdapters.poll(waitTimeoutMs, TimeUnit.MILLISECONDS);
            if (callAdapter == null) {
                throw new RuntimeException("Can't get an adapter in time");
            }
            return callAdapter.call(payload);
        } catch (Throwable t) {
            final String errorMsg =
                    String.format(
                            "Failure in handleRequest: %s", ExceptionUtils.stringifyException(t));
            logger.log(Level.WARNING, errorMsg);
            throw new RequestInvocationException(ADAPTER_INVOCATION_EXCEPTION, errorMsg);
        } finally {
            if (callAdapter != null) {
                callAdapters.add(callAdapter);
            }
        }
    }

    @Override
    public void close(byte[] payload, Context ctx) {
        try {
            for (SerDeScalarFunctionCallAdapter callAdapter : callAdapters) {
                callAdapter.close();
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Close failure: {0}", e);
        }
    }

    @VisibleForTesting
    SerDeScalarFunctionCallAdapter getCallAdapter() {
        // Just for testing, since this wouldn't be thread safe!
        return callAdapters.peek();
    }
}
