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

/** Scalar function for robust scaling of values. */
public class MLRobustScalerFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_ROBUST_SCALER";
    /** The name of the function. */
    public final String functionName;

    /**
     * Constructor for MLRobustScalerFunction.
     *
     * @param functionName The name of the function.
     */
    public MLRobustScalerFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Evaluates and returns a scaled double value based on the given value, median, first quartile,
     * and third quartile, with centering and scaling enabled by default.
     *
     * @param value the value to be scaled
     * @param median the median value
     * @param firstQuartile the first quartile value
     * @param thirdQuartile the third quartile value
     * @return the scaled double value, or null if the provided value is null
     */
    public Double eval(Object value, Object median, Object firstQuartile, Object thirdQuartile) {
        return eval(value, median, firstQuartile, thirdQuartile, true, true);
    }

    /**
     * Evaluates and returns a scaled double value based on the given value, median, first quartile,
     * third quartile, and centering option.
     *
     * @param value the value to be scaled
     * @param median the median value
     * @param firstQuartile the first quartile value
     * @param thirdQuartile the third quartile value
     * @param withCentering whether to enable centering
     * @return the scaled double value, or null if the provided value is null
     */
    public Double eval(
            Object value,
            Object median,
            Object firstQuartile,
            Object thirdQuartile,
            Object withCentering) {
        return eval(value, median, firstQuartile, thirdQuartile, withCentering, true);
    }

    /**
     * Evaluates and returns a scaled double value based on the given value, median, first quartile,
     * third quartile, centering, and scaling options.
     *
     * @param value the value to be scaled
     * @param median the median value
     * @param firstQuartile the first quartile value
     * @param thirdQuartile the third quartile value
     * @param withCentering whether to enable centering
     * @param withScaling whether to enable scaling
     * @return the scaled double value, or null if the provided value is null
     */
    public Double eval(
            Object value,
            Object median,
            Object firstQuartile,
            Object thirdQuartile,
            Object withCentering,
            Object withScaling) {
        if (Objects.isNull(value)) {
            return null;
        }
        boolean centering = (boolean) withCentering;
        boolean scaling = (boolean) withScaling;
        if (value instanceof Long
                && median instanceof Long
                && firstQuartile instanceof Long
                && thirdQuartile instanceof Long) {
            return getScaledValue(
                    getLongValue(value, NAME),
                    getLongValue(median, NAME),
                    getLongValue(firstQuartile, NAME),
                    getLongValue(thirdQuartile, NAME),
                    centering,
                    scaling);
        } else {
            return getScaledValue(
                    Objects.requireNonNull(getDoubleValue(value, NAME)),
                    getDoubleValue(median, NAME),
                    getDoubleValue(firstQuartile, NAME),
                    getDoubleValue(thirdQuartile, NAME),
                    centering,
                    scaling);
        }
    }

    private Double getScaledValue(
            Long value,
            Long median,
            Long firstQuartile,
            Long thirdQuartile,
            boolean centering,
            boolean scaling) {
        Optional<Long> IQR = Optional.empty();
        if (!centering) {
            median = 0L;
        }
        if (!scaling) {
            IQR = Optional.of(1L);
        }
        validateMetrics(median, firstQuartile, thirdQuartile, centering, scaling);
        if (!IQR.isPresent()) {
            IQR = Optional.of(thirdQuartile - firstQuartile);
        }
        return (double) (value - median) / (IQR.get() == 0L ? 1L : IQR.get());
    }

    private Double getScaledValue(
            Double value,
            Double median,
            Double firstQuartile,
            Double thirdQuartile,
            boolean centering,
            boolean scaling) {
        if (value.isNaN() || value.isInfinite()) {
            return value;
        }
        Optional<Double> IQR = Optional.empty();
        if (!centering) {
            median = 0.0;
        }
        if (!scaling) {
            IQR = Optional.of(1.0);
        }
        if (Objects.isNull(median)
                || median.isNaN()
                || median.isInfinite()
                || (!IQR.isPresent()
                        && (Objects.isNull(firstQuartile)
                                || firstQuartile.isNaN()
                                || firstQuartile.isInfinite()
                                || Objects.isNull(thirdQuartile)
                                || thirdQuartile.isNaN()
                                || thirdQuartile.isInfinite()))) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The Median, First Quartile and Third Quartile arguments to %s function cannot be a NULL, NaN or Infinite value",
                            NAME));
        }
        validateMetrics(median, firstQuartile, thirdQuartile, centering, scaling);
        if (!IQR.isPresent()) {
            IQR = Optional.of(thirdQuartile - firstQuartile);
        }
        return (value - median) / (IQR.get() == 0.0 ? 1.0 : IQR.get());
    }

    private <T extends Comparable<T>> void validateMetrics(
            T median, T firstQuartile, T thirdQuartile, boolean centering, boolean scaling) {
        if (scaling && firstQuartile.compareTo(thirdQuartile) > 0) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The First Quartile argument to %s function cannot be greater than the Third Quartile argument",
                            NAME));
        }
        if (scaling
                && centering
                && (median.compareTo(firstQuartile) < 0 || median.compareTo(thirdQuartile) > 0)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The Median argument to %s function needs to be between First Quartile and Third Quartile",
                            NAME));
        }
    }

    /**
     * Returns the type inference logic for the function.
     *
     * @param typeFactory The factory for creating data types.
     * @return The type inference logic.
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
                                        return count >= 4 && count <= 6;
                                    }

                                    @Override
                                    public Optional<Integer> getMinCount() {
                                        return Optional.of(4);
                                    }

                                    @Override
                                    public Optional<Integer> getMaxCount() {
                                        return Optional.of(6);
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
                                                            if (i < 4) {
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
                                for (int i = 0; i < 6; i++) {
                                    arguments.add(Signature.Argument.of("input" + i));
                                }
                                return Collections.singletonList(Signature.of(arguments));
                            }
                        })
                .outputTypeStrategy(callContext -> Optional.of(DataTypes.DOUBLE()))
                .build();
    }
}
