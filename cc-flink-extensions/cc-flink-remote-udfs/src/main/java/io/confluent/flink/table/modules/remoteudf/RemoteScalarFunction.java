/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.strategies.AnyArgumentBridgeToInternalTypeStrategy;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.confluent.flink.table.modules.remoteudf.RemoteUdfModule.CONFLUENT_REMOTE_UDF_TARGET;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;

/** Proof-of-concept implementation for remote scalar UDF. */
public class RemoteScalarFunction extends ScalarFunction implements SpecializedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteScalarFunction.class);

    /** The name of this function, by which it can be called from SQL. */
    public static final String NAME = "CALL_REMOTE_SCALAR";

    /** Number of metadata arguments to this function. */
    private static final int NUM_META_ARGS = 3;

    /** The UDF service gateway target address (e.g. localhost:5001). */
    private final String udfGatewayTarget;

    /** The specification of the remote UDF, e.g. class name, argument and return value types. */
    @Nullable private final RemoteUdfSpec remoteUdfSpec;

    /** Runtime to invoke the remote function. */
    private transient RemoteUdfRuntime remoteUdfRuntime;

    public RemoteScalarFunction(Map<String, String> config) {
        LOG.info("RemoteScalarFunction config: {}", config);
        String udfGatewayTarget = config.get(CONFLUENT_REMOTE_UDF_TARGET.key());

        if (udfGatewayTarget != null && !udfGatewayTarget.isEmpty()) {
            this.udfGatewayTarget = udfGatewayTarget;
        } else {
            // TODO: remove hardcoded proxy
            this.udfGatewayTarget = "udf-proxy.udf-proxy:50051";
        }

        this.remoteUdfSpec = null;
    }

    private RemoteScalarFunction(String udfGatewayTarget, RemoteUdfSpec remoteUdfSpec) {
        this.udfGatewayTarget = udfGatewayTarget;
        this.remoteUdfSpec = remoteUdfSpec;
    }

    /**
     * Calls the given remote function of given return type with the given payload and returns the
     * return value.
     *
     * @param functionId registration id of the remote function to call.
     * @param functionClass the name of the function class to call.
     * @param rtype return type of the remote function to call (INT, DOUBLE, STRING).
     * @param args arguments for the remote function call.
     * @return the return value of the remote UDF execution.
     */
    public @Nullable Object eval(
            String functionId, String functionClass, String rtype, Object... args)
            throws Exception {

        LOG.debug(
                "Invoking remote scalar function. Handler: {}, Function: {}, Rtype: {}, Args: {}",
                functionId,
                functionClass,
                rtype,
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
        return TypeInference.newBuilder()
                // specify a strategy for the result data type of the function
                .inputTypeStrategy(
                        varyingSequence(
                                // 3 meta args for from the call
                                logical(LogicalTypeFamily.CHARACTER_STRING),
                                logical(LogicalTypeFamily.CHARACTER_STRING),
                                logical(LogicalTypeFamily.CHARACTER_STRING),
                                // ANY type, bridged to internal type
                                new AnyArgumentBridgeToInternalTypeStrategy()))
                .outputTypeStrategy(
                        callContext -> {
                            if (!callContext.isArgumentLiteral(2)
                                    || callContext.isArgumentNull(2)) {
                                throw callContext.newValidationError(
                                        "Literal expected for second argument.");
                            }
                            // return a data type based on a literal
                            final String literal =
                                    callContext.getArgumentValue(2, String.class).orElse("STRING");
                            return Optional.of(typeFactory.createDataType(literal))
                                    .map(DataType::toInternal);
                        })
                .build();
    }

    @Override
    public UserDefinedFunction specialize(SpecializedContext context) {
        CallContext callContext = context.getCallContext();
        List<DataType> ctxArgumentTypeList = callContext.getArgumentDataTypes();

        int size = ctxArgumentTypeList.size() - NUM_META_ARGS;
        List<DataType> argumentTypes = new ArrayList<>(size);
        String functionId = callContext.getArgumentValue(0, String.class).get();
        String functionClass = callContext.getArgumentValue(1, String.class).get();
        DataType returnType = callContext.getOutputDataType().get();

        // Skip the meta arguments, they are not part of the payload and their data types don't
        // matter.
        for (int i = NUM_META_ARGS; i < ctxArgumentTypeList.size(); ++i) {
            argumentTypes.add(ctxArgumentTypeList.get(i));
        }

        RemoteUdfSpec remoteUdfSpec =
                new RemoteUdfSpec(functionId, functionClass, returnType, argumentTypes);

        // Return the specialized instance
        return new RemoteScalarFunction(udfGatewayTarget, remoteUdfSpec);
    }
}
