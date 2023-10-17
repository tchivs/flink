/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import io.confluent.flink.table.utils.Base64SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ObjectInputStream;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;

/** Proof-of-concept implementation for remote scalar UDF. */
public class RemoteScalarFunction extends RemoteScalarFunctionBase {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteScalarFunction.class);

    /** The name of this function, by which it can be called from SQL. */
    public static final String NAME = "CALL_REMOTE_SCALAR";

    public RemoteScalarFunction(Map<String, String> config) {
        super(config);
    }

    /**
     * Calls the given remote function of given return type with the given payload and returns the
     * return value.
     *
     * @param function name of the remote function to call.
     * @param rtype return type of the remote function to call (INT, DOUBLE, STRING).
     * @param args arguments for the remote function call.
     * @return the return value of the remote UDF execution.
     */
    public @Nullable Object eval(String function, String rtype, Object... args) throws Exception {

        LOG.debug(
                "Invoking remote scalar function. Function: {}, Rtype: {}, Args: {}",
                function,
                rtype,
                args);

        String encodedPayload = Base64SerializationUtil.serialize((oos) -> oos.writeObject(args));
        UdfGatewayOuterClass.InvokeRequest request =
                UdfGatewayOuterClass.InvokeRequest.newBuilder()
                        .setFuncName(function)
                        .setPayload(encodedPayload)
                        .build();
        UdfGatewayOuterClass.InvokeResponse response = getUdfGateway().invoke(request);
        return Base64SerializationUtil.deserialize(
                response.getPayload(), ObjectInputStream::readObject);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                // specify a strategy for the result data type of the function
                .inputTypeStrategy(
                        varyingSequence(
                                logical(LogicalTypeFamily.CHARACTER_STRING),
                                logical(LogicalTypeFamily.CHARACTER_STRING),
                                ANY))
                .outputTypeStrategy(
                        callContext -> {
                            if (!callContext.isArgumentLiteral(1)
                                    || callContext.isArgumentNull(1)) {
                                throw callContext.newValidationError(
                                        "Literal expected for second argument.");
                            }
                            // return a data type based on a literal
                            final String literal =
                                    callContext.getArgumentValue(1, String.class).orElse("STRING");
                            switch (literal) {
                                case "INT":
                                    return Optional.of(DataTypes.INT());
                                case "DOUBLE":
                                    return Optional.of(DataTypes.DOUBLE());
                                case "STRING":
                                default:
                                    return Optional.of(DataTypes.STRING());
                            }
                        })
                .build();
    }
}
