/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.service.ServiceTasksOptions;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Base class for remote {@link org.apache.flink.table.functions.ScalarFunction}. */
public abstract class RemoteScalarFunctionBase extends ScalarFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteScalarFunctionBase.class);

    /** Target address for the udfGateway. */
    private final String udfGatewayTarget;

    /** The managed channel used to construct the udfGateway. */
    private ManagedChannel channel;

    /** Gateway to invoke remote UDFs. */
    private UdfGatewayGrpc.UdfGatewayBlockingStub udfGateway;

    public RemoteScalarFunctionBase(Map<String, String> config) {
        LOG.info("RemoteScalarFunction config: {}", config);
        String udfGatewayTarget = config.get(ServiceTasksOptions.CONFLUENT_REMOTE_UDF_TARGET.key());
        if (udfGatewayTarget != null && !udfGatewayTarget.isEmpty()) {
            this.udfGatewayTarget = udfGatewayTarget;
        } else {
            // TODO: remove hardcoded proxy
            this.udfGatewayTarget = "udf-proxy.udf-proxy:50051";
        }
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        Preconditions.checkArgument(!udfGatewayTarget.isEmpty(), "Gateway target not configured!");
        channel =
                Preconditions.checkNotNull(
                        ManagedChannelBuilder.forTarget(udfGatewayTarget).usePlaintext().build());
        udfGateway = Preconditions.checkNotNull(UdfGatewayGrpc.newBlockingStub(channel));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    protected UdfGatewayGrpc.UdfGatewayBlockingStub getUdfGateway() {
        return udfGateway;
    }
}
