/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import java.util.Map;

/** Proof-of-concept implementation for (String -> String) remote scalar UDF. */
public class RemoteStringScalarFunction extends RemoteScalarFunctionBase {

    /** The name of this function, by which it can be called from SQL. */
    public static final String NAME = "CALL_REMOTE_SCALAR_STR";

    public RemoteStringScalarFunction(Map<String, String> config) {
        super(config);
    }

    /**
     * Calls the given remote function with the given payload and returns the payload from the
     * response.
     *
     * @param function name of the remote function to call.
     * @param payload payload as argument for the remote function.
     * @return payload of the response as return value of the remote function invocation.
     */
    public String eval(String function, String payload) {
        UdfGatewayOuterClass.InvokeRequest request =
                UdfGatewayOuterClass.InvokeRequest.newBuilder()
                        .setFuncName(function)
                        .setPayload(payload)
                        .build();
        UdfGatewayOuterClass.InvokeResponse response = getUdfGateway().invoke(request);
        return response.getPayload();
    }
}
