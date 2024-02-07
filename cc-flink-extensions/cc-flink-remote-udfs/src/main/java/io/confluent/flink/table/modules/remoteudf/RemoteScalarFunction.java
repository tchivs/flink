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

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_TARGET;

/** Proof-of-concept implementation for remote scalar UDF. */
public class RemoteScalarFunction extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteScalarFunction.class);

    /** The UDF service gateway target address (e.g. localhost:5001). */
    private final String udfGatewayTarget;

    /** The specification of the remote UDF, e.g. class name, argument and return value types. */
    private final RemoteUdfSpec remoteUdfSpec;

    /** Runtime to invoke the remote function. */
    private transient RemoteUdfRuntime remoteUdfRuntime;

    private RemoteScalarFunction(String udfGatewayTarget, RemoteUdfSpec remoteUdfSpec) {
        Preconditions.checkNotNull(udfGatewayTarget);
        Preconditions.checkNotNull(remoteUdfSpec);
        this.udfGatewayTarget = udfGatewayTarget;
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
                "Invoking remote scalar function. Handler: {}, Function: {}, Rtype: {}, Args: {}",
                remoteUdfSpec.getFunctionId(),
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
        this.remoteUdfRuntime = RemoteUdfRuntime.open(udfGatewayTarget, remoteUdfSpec);
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
        String udfGatewayTarget = config.get(CONFLUENT_REMOTE_UDF_TARGET.key());

        if (udfGatewayTarget == null || udfGatewayTarget.isEmpty()) {
            // TODO: remove hardcoded proxy
            udfGatewayTarget = "udf-proxy.udf-proxy:50051";
        }
        return new RemoteScalarFunction(udfGatewayTarget, remoteUdfSpec);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
