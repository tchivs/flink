/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Clock;
import java.util.Map;

/** Proof-of-concept implementation for remote scalar UDF. */
public class RemoteScalarFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteScalarFunction.class);

    /** Config map for the remote UDF runtime. */
    private final Map<String, String> configMap;

    /** The specification of the remote UDF, e.g. class name, argument and return value types. */
    private final RemoteUdfSpec remoteUdfSpec;

    /** Runtime to invoke the remote function. */
    private transient RemoteUdfRuntime remoteUdfRuntime;

    /** Tracks events which occur with UDFs. */
    private transient RemoteUdfMetrics metrics;

    /** Clock used for timing. Can be mocked in testing */
    private Clock clock;

    private RemoteScalarFunction(
            Map<String, String> confMap, RemoteUdfSpec remoteUdfSpec, Clock clock) {
        Preconditions.checkNotNull(confMap);
        Preconditions.checkNotNull(remoteUdfSpec);
        this.configMap = confMap;
        this.remoteUdfSpec = remoteUdfSpec;
        this.clock = clock;
    }

    /**
     * Calls the given remote function of given return type with the given payload and returns the
     * return value.
     *
     * @param args arguments for the remote function call.
     * @return the return value of the remote UDF execution.
     */
    public @Nullable Object eval(Object... args) throws Exception {
        LOG.debug(
                "Invoking remote scalar function. Plugin: {}, Function: {}, Rtype: {}, Args: {}",
                remoteUdfSpec.getPluginId(),
                remoteUdfSpec.getFunctionClassName(),
                remoteUdfSpec.getReturnType(),
                args);

        long startMs = clock.millis();
        try {
            metrics.invocation();
            Object result = remoteUdfRuntime.callRemoteUdf(args);
            metrics.invocationSuccess();
            return result;
        } catch (Throwable t) {
            LOG.error("Got an error while doing UDF call", t);
            metrics.invocationFailure();
            throw t;
        } finally {
            long completeMs = clock.millis();
            metrics.invocationMs(completeMs - startMs);
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        this.metrics = new RemoteUdfMetrics(context.getMetricGroup());

        // Open should never be called twice, but just in case...
        if (this.remoteUdfRuntime != null) {
            remoteUdfRuntime.close();
        }

        metrics.instanceProvision();
        Preconditions.checkNotNull(remoteUdfSpec);
        this.remoteUdfRuntime = RemoteUdfRuntime.open(configMap, remoteUdfSpec, metrics);
    }

    @Override
    public void close() throws Exception {
        if (remoteUdfRuntime != null) {
            metrics.instanceDeprovision();
            this.remoteUdfRuntime.close();
        }
        super.close();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return UdfUtil.getTypeInference(
                remoteUdfSpec.getArgumentTypes(), remoteUdfSpec.getReturnType());
    }

    public static RemoteScalarFunction create(
            Map<String, String> config, RemoteUdfSpec remoteUdfSpec) {
        LOG.info("RemoteScalarFunction config: {}", config);
        return new RemoteScalarFunction(config, remoteUdfSpec, Clock.systemUTC());
    }

    @Override
    public boolean canReduceExpression() {
        return false;
    }

    @Override
    public boolean isDeterministic() {
        return remoteUdfSpec.isDeterministic();
    }
}
