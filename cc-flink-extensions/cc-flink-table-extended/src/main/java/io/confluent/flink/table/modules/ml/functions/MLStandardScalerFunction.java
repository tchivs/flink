/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.confluent.flink.table.utils.mlutils.MlFunctionsUtil.getDoubleValue;
import static io.confluent.flink.table.utils.mlutils.MlFunctionsUtil.getLongValue;
import static io.confluent.flink.table.utils.mlutils.MlFunctionsUtil.getTypeInferenceForScalerFunctions;

/** Scalar function for standard scaling of values. */
public class MLStandardScalerFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_STANDARD_SCALER";
    /** The name of the function. */
    public final String functionName;

    /**
     * Constructor for MLStandardScalerFunction.
     *
     * @param functionName The name of the function.
     */
    public MLStandardScalerFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Evaluates the scaling of a value using mean and standard deviation.
     *
     * @param value The value to be scaled.
     * @param mean The mean value.
     * @param stdDev The standard deviation.
     * @return The scaled value.
     */
    public Double eval(Object value, Object mean, Object stdDev) {
        return eval(value, mean, stdDev, true, true);
    }

    /**
     * Evaluates the scaling of a value using mean and standard deviation with centering option.
     *
     * @param value The value to be scaled.
     * @param mean The mean value.
     * @param stdDev The standard deviation.
     * @param withCentering Whether to center the data.
     * @return The scaled value.
     */
    public Double eval(Object value, Object mean, Object stdDev, Object withCentering) {
        return eval(value, mean, stdDev, withCentering, true);
    }

    /**
     * Evaluates the scaling of a value using mean and standard deviation with centering and scaling
     * options.
     *
     * @param value The value to be scaled.
     * @param mean The mean value.
     * @param stdDev The standard deviation.
     * @param withCentering Whether to center the data.
     * @param withScaling Whether to scale the data.
     * @return The scaled value.
     */
    public Double eval(
            Object value, Object mean, Object stdDev, Object withCentering, Object withScaling) {
        if (Objects.isNull(value)) {
            return null;
        }
        boolean center = (boolean) withCentering;
        boolean scale = (boolean) withScaling;
        if (value instanceof Long && mean instanceof Long) {
            return getScaledValue(
                    getLongValue(value, NAME),
                    getLongValue(mean, NAME),
                    getDoubleValue(stdDev, NAME),
                    center,
                    scale);
        }
        return getScaledValue(
                Objects.requireNonNull(getDoubleValue(value, NAME)),
                getDoubleValue(mean, NAME),
                getDoubleValue(stdDev, NAME),
                center,
                scale);
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
                    String.format("The Mean argument to %s function cannot be NaN or NULL", NAME));
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
     * Returns the type inference logic for the function.
     *
     * @param typeFactory The factory for creating data types.
     * @return The type inference logic.
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return getTypeInferenceForScalerFunctions(
                3,
                5,
                DataTypes.DOUBLE(),
                count -> count >= 3 && count <= 5,
                this::validateInputTypes);
    }

    private Optional<String> validateInputTypes(List<DataType> argumentDataTypes) {
        return IntStream.range(0, argumentDataTypes.size())
                .mapToObj(
                        i -> {
                            DataType argDataType = argumentDataTypes.get(i);
                            if (i < 3) {
                                if (!argDataType.equals(DataTypes.NULL())
                                        && !argDataType
                                                .getLogicalType()
                                                .isAnyOf(
                                                        LogicalTypeFamily.NUMERIC,
                                                        LogicalTypeFamily.TIMESTAMP,
                                                        LogicalTypeFamily.TIME,
                                                        LogicalTypeFamily.INTERVAL,
                                                        LogicalTypeFamily.DATETIME)) {
                                    return String.format(
                                            "%s datatype is not supported as argument %s to %s function. Please refer documentation for supported datatypes",
                                            i, argDataType, NAME);
                                }
                            } else {
                                if (!argDataType.nullable().equals(DataTypes.BOOLEAN())) {
                                    return String.format(
                                            "Arguments withCentering and withScaling to %s function need to be of type Boolean",
                                            NAME);
                                }
                            }
                            return null;
                        })
                .filter(Objects::nonNull)
                .findFirst();
    }
}
