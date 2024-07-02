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
import static io.confluent.flink.table.utils.mlutils.MlFunctionsUtil.getTypeInferenceForMLScalarFunctions;

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
     * Evaluates and returns a scaled double value based on the given value and absolute maximum
     * value.
     *
     * @param value the value to be scaled; can be of any type that can be converted to a double
     * @param absMax the absolute maximum factor; can be of any type that can be converted to a
     *     double
     * @return the scaled double value, or null if the provided value is null
     * @throws FlinkRuntimeException if any error occurs during the processing
     */
    public Double eval(Object value, Object absMax) {
        try {
            if (Objects.isNull(value)) {
                return null;
            }
            return getScaledValue(
                    Objects.requireNonNull(getDoubleValue(value, NAME)),
                    getDoubleValue(absMax, NAME));
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
                            "The Absolute Maximum argument to %s function cannot be NaN or NULL.",
                            NAME));
        }
        if (absMax.isInfinite()) {
            return 0.0;
        }
        // sets absMax to its absolute value if negative
        absMax = Math.abs(absMax);
        return value > absMax
                ? 1.0
                : (value < -absMax ? -1.0 : value / (absMax == 0.0 ? 1.0 : absMax));
    }

    /**
     * Provides the type inference logic for this function.
     *
     * @param typeFactory the DataTypeFactory instance.
     * @return the TypeInference instance.
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return getTypeInferenceForMLScalarFunctions(
                2, 2, DataTypes.DOUBLE(), count -> count == 2, this::validateInputTypes);
    }

    private Optional<String> validateInputTypes(List<DataType> argumentDataTypes) {
        return IntStream.range(0, argumentDataTypes.size())
                .mapToObj(
                        i -> {
                            DataType argDataType = argumentDataTypes.get(i);
                            // Check if the data type is supported
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
                                        "%s datatype is not supported as first argument to %s function. Please refer documentation for supported datatypes",
                                        argDataType, NAME);
                            }
                            // Check if absolute Maximum value
                            // is null
                            if (i > 0 && argDataType.equals(DataTypes.NULL())) {
                                return String.format(
                                        "Second argument to %s function cannot be NULL", NAME);
                            }
                            return null;
                        })
                .filter(Objects::nonNull)
                .findFirst();
    }
}
