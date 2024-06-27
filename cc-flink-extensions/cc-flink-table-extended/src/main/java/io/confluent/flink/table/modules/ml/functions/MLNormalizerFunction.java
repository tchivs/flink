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
import static io.confluent.flink.table.utils.mlutils.MlFunctionsUtil.getTypeInferenceForScalerFunctions;

/** Scalar function for normalizing values. */
public class MLNormalizerFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_NORMALIZER";
    /** The name of the function. */
    public final String functionName;

    /**
     * Constructor for MLNormalizerFunction.
     *
     * @param functionName The name of the function.
     */
    public MLNormalizerFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Evaluates and returns a scaled double value based on the given value and normalization
     * factor.
     *
     * @param value the value to be scaled; can be of any type that can be converted to a double
     * @param normFactor the normalization factor; can be of any type that can be converted to a
     *     double
     * @return the scaled double value, or null if the provided value is null
     * @throws FlinkRuntimeException if any error occurs during the processing
     */
    public Double eval(Object value, Object normFactor) {
        try {
            if (Objects.isNull(value)) {
                return null;
            }
            return getScaledValue(
                    Objects.requireNonNull(getDoubleValue(value, NAME)),
                    getDoubleValue(normFactor, NAME));
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    /**
     * Scales the value by the normalization factor.
     *
     * @param value The value to be normalized.
     * @param normValue The normalization factor.
     * @return The normalized value.
     */
    private Double getScaledValue(Double value, Double normValue) {
        if (value.isNaN() || value.isInfinite()) {
            return value;
        }
        if (Objects.isNull(normValue) || normValue.isNaN()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The Normalization Factor argument to %s function cannot be NaN or NULL.",
                            NAME));
        }
        if (normValue.isInfinite()) {
            return 0.0;
        }
        return normValue == 0.0 ? value : (value / normValue);
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
