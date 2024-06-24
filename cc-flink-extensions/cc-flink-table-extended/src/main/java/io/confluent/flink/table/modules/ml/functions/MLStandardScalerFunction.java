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
import static io.confluent.flink.table.utils.mlutils.MlFunctionsUtil.getLongValue;

/** A scalar function that performs standard scaling on input values. */
public class MLStandardScalerFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_STANDARD_SCALER";
    /** The name of the function. */
    public final String functionName;

    /**
     * Constructor for the MLStandardScalerFunction.
     *
     * @param functionName The name of the function.
     */
    public MLStandardScalerFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Evaluates the standard scaling function.
     *
     * @param args input arguments where: - args[0] is the input value, - args[1] is the mean value
     *     of the feature, args[2] is the standard deviation value of the feature, args[3] is
     *     withCentering boolean value args[4] is withScaling boolean value.
     * @return The scaled value.
     */
    public Double eval(Object... args) {
        try {
            if (Objects.isNull(args[0])) {
                return null;
            }
            Object value = args[0];
            Object mean = args[1];
            Object stdDev = args[2];
            boolean withCentering = true;
            boolean withScaling = true;
            if (args.length >= 4) {
                withCentering = (boolean) args[3];
                withScaling = args.length == 5 ? (boolean) args[4] : withScaling;
            }
            if (value instanceof Long && mean instanceof Long) {
                return getScaledValue(
                        Objects.requireNonNull(getLongValue(value, NAME)),
                        getLongValue(mean, NAME),
                        getDoubleValue(stdDev, NAME),
                        withCentering,
                        withScaling);
            }
            return getScaledValue(
                    Objects.requireNonNull(getDoubleValue(value, NAME)),
                    getDoubleValue(mean, NAME),
                    getDoubleValue(stdDev, NAME),
                    withCentering,
                    withScaling);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    private Double getScaledValue(
            Double value, Double mean, Double stdDev, boolean withCentering, boolean withScaling) {
        if (value.isNaN() || value.isInfinite()) {
            return value;
        }
        if (!withCentering) {
            mean = 0.0;
        }
        stdDev = validateStdDeviation(stdDev, withScaling);
        if (Objects.isNull(mean) || mean.isNaN() || mean.isInfinite()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The Mean argument to %s function cannot be NaN, NULL and Infinite",
                            NAME));
        }
        return stdDev.isInfinite() ? 0.0 : (value - mean) / stdDev;
    }

    private Double getScaledValue(
            Long value, Long mean, Double stdDev, boolean withCentering, boolean withScaling) {
        if (!withCentering) {
            mean = 0L;
        }
        stdDev = validateStdDeviation(stdDev, withScaling);
        if (Objects.isNull(mean)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The Mean argument to %s function cannot be NaN, NULL or Infinite",
                            NAME));
        }
        return stdDev.isInfinite() ? 0.0 : (double) (value - mean) / stdDev;
    }

    private Double validateStdDeviation(Double stdDev, boolean withScaling) {
        if (!withScaling || (Objects.nonNull(stdDev) && stdDev == 0.0)) {
            stdDev = 1.0;
        }
        if (Objects.isNull(stdDev) || stdDev.isNaN()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The Standard Deviation argument to %s function cannot be NaN or NULL",
                            NAME));
        }
        return stdDev;
    }

    /**
     * Returns the type inference logic for this scalar function.
     *
     * @param typeFactory the data type factory
     * @return the type inference
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
                                        return count > 2 && count < 6;
                                    }

                                    @Override
                                    public Optional<Integer> getMinCount() {
                                        return Optional.of(3);
                                    }

                                    @Override
                                    public Optional<Integer> getMaxCount() {
                                        return Optional.of(5);
                                    }
                                };
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                List<DataType> argumentDataTypes =
                                        callContext.getArgumentDataTypes();
                                Optional<String> errorMessage =
                                        IntStream.range(0, argumentDataTypes.size())
                                                .mapToObj(
                                                        i -> {
                                                            DataType argDataType =
                                                                    argumentDataTypes.get(i);
                                                            if (i < 3) {
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
                                                                            "%s datatype is not supported as argument %s to %s function. Please refer documentation for supported datatypes",
                                                                            i, argDataType, NAME);
                                                                }
                                                            } else {
                                                                if (!argDataType
                                                                        .nullable()
                                                                        .equals(
                                                                                DataTypes
                                                                                        .BOOLEAN())) {
                                                                    return String.format(
                                                                            "Arguments withCentering and withScaling to %s function need to be of type Boolean",
                                                                            NAME);
                                                                }
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
                                for (int i = 0; i < 5; i++) {
                                    arguments.add(Signature.Argument.of("input" + i));
                                }
                                return Collections.singletonList(Signature.of(arguments));
                            }
                        })
                .outputTypeStrategy(callContext -> Optional.of(DataTypes.DOUBLE()))
                .build();
    }
}
