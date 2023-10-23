/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import io.confluent.flink.table.utils.Base64SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.inference.InputTypeStrategies.ANY;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;

/** Proof-of-concept implementation for remote scalar UDF. */
public class RemoteScalarFunction extends RemoteScalarFunctionBase implements SpecializedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteScalarFunction.class);

    /** The name of this function, by which it can be called from SQL. */
    public static final String NAME = "CALL_REMOTE_SCALAR";

    /** Number of metadata arguments to this function. */
    private static final int NUM_META_ARGS = 3;

    private final List<DataType> argumentTypes;
    private final List<Class<?>> argumentClasses;
    private final String callerUUID;

    private String serializedArgumentClasses;

    public RemoteScalarFunction(Map<String, String> config) {
        super(config);
        this.argumentTypes = new ArrayList<>();
        this.argumentClasses = new ArrayList<>();
        this.callerUUID = UUID.randomUUID().toString();
    }

    /**
     * Calls the given remote function of given return type with the given payload and returns the
     * return value.
     *
     * @param handler name of the remote function to call.
     * @param functionClass the name of the function class to call.
     * @param rtype return type of the remote function to call (INT, DOUBLE, STRING).
     * @param args arguments for the remote function call.
     * @return the return value of the remote UDF execution.
     */
    public @Nullable Object eval(String handler, String functionClass, String rtype, Object... args)
            throws Exception {

        LOG.debug(
                "Invoking remote scalar function. Handler: {}, Function: {}, Rtype: {}, Args: {}",
                handler,
                functionClass,
                rtype,
                args);

        String encodedArgs = Base64SerializationUtil.serialize((oos) -> oos.writeObject(args));

        String payload =
                '"'
                        + callerUUID
                        + " "
                        + encodedArgs
                        + " "
                        + functionClass
                        + " "
                        + serializedArgumentClasses
                        + '"';

        UdfGatewayOuterClass.InvokeRequest request =
                UdfGatewayOuterClass.InvokeRequest.newBuilder()
                        .setFuncName(handler)
                        .setPayload(payload)
                        .build();
        UdfGatewayOuterClass.InvokeResponse response = getUdfGateway().invoke(request);
        String result = response.getPayload();
        result = result.substring(1, result.length() - 1);
        return Base64SerializationUtil.deserialize(result, ObjectInputStream::readObject);
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
                                ANY))
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
                            return Optional.of(typeFactory.createDataType(literal));
                        })
                .build();
    }

    @Override
    public UserDefinedFunction specialize(SpecializedContext context) {
        this.argumentTypes.clear();
        this.argumentClasses.clear();
        List<DataType> argumentDataTypes = context.getCallContext().getArgumentDataTypes();
        // Skip the meta arguments, they are not part of the payload and their data types don't
        // matter.
        for (int i = NUM_META_ARGS; i < argumentDataTypes.size(); ++i) {
            argumentTypes.add(argumentDataTypes.get(i));
        }

        this.argumentTypes.stream()
                .map(DataType::getConversionClass)
                .collect(Collectors.toCollection(() -> this.argumentClasses));
        try {
            this.serializedArgumentClasses =
                    Base64SerializationUtil.serialize((oos) -> oos.writeObject(argumentClasses));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // TODO: With this information we can also setup proper SQL type serialization instead of
        //  using Java serialization.
        return this;
    }
}
