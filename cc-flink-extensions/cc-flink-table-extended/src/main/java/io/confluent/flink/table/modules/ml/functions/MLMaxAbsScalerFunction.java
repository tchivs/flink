/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.confluent.flink.table.utils.mlutils.MlFunctionsUtil.getDoubleValue;

/**
 * A ScalarFunction that implements the ML_MAX_ABS_SCALER function. This function scales the input
 * value by the absolute maximum value provided.
 */
public class MLMaxAbsScalerFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_MAX_ABS_SCALER";
    /** The name of the function. */
    public final String functionName;

    /**
     * Constructor for the MLMaxAbsScaleFunction class.
     *
     * @param functionName the name of the function.
     */
    public MLMaxAbsScalerFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Evaluates the function with the given arguments.
     *
     * @param args the input arguments where: - args[0] is the input value, - args[1] is the
     *     absolute maximum value of the feature.
     * @return the scaled value as a Double.
     */
    public Double eval(Object... args) {
        try {
            if (Objects.isNull(args[0])) {
                return null;
            }
            Object value = args[0];
            Object absMax = args[1];
            return getScaledValue(getDoubleValue(value, NAME), getDoubleValue(absMax, NAME));
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    /**
     * Scales the given value by the absolute maximum value.
     *
     * @param value the value to scale.
     * @param absMax the absolute maximum value.
     * @return the scaled value.
     * @throws FlinkRuntimeException if absMax is NaN or null.
     */
    private Double getScaledValue(Double value, Double absMax) {
        if (value.isNaN() || value.isInfinite()) {
            return value;
        }
        if (Objects.isNull(absMax) || absMax.isNaN()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The Absolute Maximum argument to %s function cannot be NaN or NULL.", NAME));
        }
        if (absMax.isInfinite()) {
            return 0.0;
        }
        // sets absMax to its absolute value if negative
        absMax = Math.abs(absMax);
        return value > absMax ? 1.0 : (value < -absMax ? -1.0 : value / (absMax == 0.0 ? 1.0:absMax));
    }

    /**
     * Provides the type inference logic for this function.
     *
     * @param typeFactory the DataTypeFactory instance.
     * @return the TypeInference instance.
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        new InputTypeStrategy() {
                            @Override
                            public ArgumentCount getArgumentCount() {
                                return new ArgumentCount() {

                                    @Override
                                    public boolean isValidCount(int count) {
                                        return count == 2;
                                    }

                                    @Override
                                    public Optional<Integer> getMinCount() {
                                        return Optional.of(2);
                                    }

                                    @Override
                                    public Optional<Integer> getMaxCount() {
                                        return Optional.of(2);
                                    }
                                };
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                List<DataType> argumentDataTypes =
                                        callContext.getArgumentDataTypes();
                                // Use IntStream.range to create indices for elements in
                                // argsDataTypes list and returns errorMessage if any
                                Optional<String> errorMessage =
                                        IntStream.range(0, argumentDataTypes.size())
                                                .mapToObj(
                                                        i -> {
                                                            DataType argDataType =
                                                                    argumentDataTypes.get(i);
                                                            // Check if the data type is supported
                                                            if (!argDataType.equals(
                                                                            DataTypes.NULL())
                                                                    && !argDataType
                                                                            .getLogicalType()
                                                                            .isAnyOf(
                                                                                    LogicalTypeFamily
                                                                                            .NUMERIC,
                                                                                    LogicalTypeFamily
                                                                                            .TIMESTAMP,
                                                                                    LogicalTypeFamily
                                                                                            .TIME,
                                                                                    LogicalTypeFamily
                                                                                            .INTERVAL,
                                                                                    LogicalTypeFamily
                                                                                            .DATETIME)) {
                                                                return String.format(
                                                                        "%s datatype is not supported as first argument to %s function. Please refer documentation for supported datatypes",
                                                                        argDataType, NAME);
                                                            }
                                                            // Check if absolute Maximum value
                                                            // is null
                                                            if (i > 0
                                                                    && argDataType.equals(
                                                                            DataTypes.NULL())) {
                                                                return String.format(
                                                                        "Second argument to %s function cannot be NULL",
                                                                        NAME);
                                                            }
                                                            return null;
                                                        })
                                                .filter(Objects::nonNull)
                                                .findFirst();

                                if (errorMessage.isPresent()) {
                                    if (throwOnFailure) {
                                        throw new ValidationException(errorMessage.get());
                                    } else {
                                        return Optional.empty();
                                    }
                                }
                                return Optional.of(argumentDataTypes);
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                final List<Signature.Argument> arguments = new ArrayList<>();
                                for (int i = 0; i < 2; i++) {
                                    arguments.add(Signature.Argument.of("input" + i));
                                }
                                return Collections.singletonList(Signature.of(arguments));
                            }
                        })
                .outputTypeStrategy(callContext -> Optional.of(DataTypes.DOUBLE()))
                .build();
    }
}
