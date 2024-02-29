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

    private RemoteScalarFunction(Map<String, String> confMap, RemoteUdfSpec remoteUdfSpec) {
        Preconditions.checkNotNull(confMap);
        Preconditions.checkNotNull(remoteUdfSpec);
        this.configMap = confMap;
        this.remoteUdfSpec = remoteUdfSpec;
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

        return remoteUdfRuntime.callRemoteUdf(args);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        // Open should never be called twice, but just in case...
        if (this.remoteUdfRuntime != null) {
            remoteUdfRuntime.close();
        }

        Preconditions.checkNotNull(remoteUdfSpec);
        this.remoteUdfRuntime = RemoteUdfRuntime.open(configMap, remoteUdfSpec);
    }

    @Override
    public void close() throws Exception {
        if (remoteUdfRuntime != null) {
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
        return new RemoteScalarFunction(config, remoteUdfSpec);
    }

    @Override
    public boolean canReduceExpression() {
        return false;
    }
}
