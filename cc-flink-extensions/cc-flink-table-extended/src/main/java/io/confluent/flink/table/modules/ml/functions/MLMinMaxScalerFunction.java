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

/** A scalar function for performing Min-Max scaling on numerical values. */
public class MLMinMaxScalerFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_MIN_MAX_SCALER";
    /** The name of the function. */
    public final String functionName;

    /**
     * Constructs a new {@code MLMinMaxScalarFunction} instance with the specified function name.
     *
     * @param functionName the name of the function
     */
    public MLMinMaxScalerFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Scales the input value to [0, 1] using Min-Max scaling.
     *
     * @param args input arguments (value, dataMin, dataMax)
     * @return the scaled value
     */
    public Double eval(Object... args) {
        if (Objects.isNull(args[0])) {
            return null;
        }
        Object value = args[0];
        Object min = args[1];
        Object max = args[2];
        if (value instanceof Long && min instanceof Long && max instanceof Long) {
            return getStandardizedValue(
                    getLongValue(value, NAME), getLongValue(min, NAME), getLongValue(max, NAME));
        }
        return getStandardizedValue(
                getDoubleValue(value, NAME), getDoubleValue(min, NAME), getDoubleValue(max, NAME));
    }

    private Double getStandardizedValue(Double value, Double dataMin, Double dataMax) {
        if (dataMax < dataMin) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The max argument to %s function has to be greater than or equal to min argument",
                            NAME));
        }
        if (dataMin.isNaN() || dataMin.isInfinite() || dataMax.isNaN() || dataMax.isInfinite()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The min and max arguments to %s function cannot be NaN or Infinite value",
                            NAME));
        }
        if (value.isInfinite() || value.isNaN()) {
            return value;
        }
        if (value > dataMax) {
            return 1.0;
        }
        if (value < dataMin) {
            return 0.0;
        }
        double range = dataMax - dataMin;
        range = range == 0.0 ? 1.0 : range;
        return (value - dataMin) / range;
    }

    private Double getStandardizedValue(Long value, Long dataMin, Long dataMax) {
        if (dataMax < dataMin) {
            throw new FlinkRuntimeException(
                    String.format(
                            "The max argument to %s function has to be greater than or equal to min argument",
                            NAME));
        }
        if (value > dataMax) {
            return 1.0;
        }
        if (value < dataMin) {
            return 0.0;
        }
        long range = dataMax - dataMin;
        range = range == 0L ? 1L : range;
        return ((double) (value - dataMin)) / range;
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
                                        return count == 3;
                                    }

                                    @Override
                                    public Optional<Integer> getMinCount() {
                                        return Optional.of(3);
                                    }

                                    @Override
                                    public Optional<Integer> getMaxCount() {
                                        return Optional.of(3);
                                    }
                                };
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                List<DataType> argsDataTypes = callContext.getArgumentDataTypes();

                                // Use IntStream.range to create indices for elements in
                                // argsDataTypes list
                                Optional<String> errorMessage =
                                        IntStream.range(0, argsDataTypes.size())
                                                .mapToObj(
                                                        i -> {
                                                            DataType argDataType =
                                                                    argsDataTypes.get(i);
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
                                                                        "%s datatype is not supported as argument to %s function. Please refer documentation for supported datatypes",
                                                                        argDataType, NAME);
                                                            }

                                                            // Check if dataMin and dataMax value
                                                            // are null
                                                            if (i > 0
                                                                    && argDataType.equals(
                                                                            DataTypes.NULL())) {
                                                                return String.format(
                                                                        "The min and max arguments to %s function cannot be null",
                                                                        NAME);
                                                            }

                                                            return null; // Return null if no error
                                                        })
                                                .filter(Objects::nonNull) // Filter out null error
                                                // messages
                                                .findFirst(); // Find the first error message

                                if (errorMessage.isPresent()) {
                                    if (throwOnFailure) {
                                        throw new ValidationException(errorMessage.get());
                                    } else {
                                        return Optional.empty();
                                    }
                                }
                                return Optional.of(argsDataTypes);
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                final List<Signature.Argument> arguments = new ArrayList<>();
                                for (int i = 0; i < 3; i++) {
                                    arguments.add(Signature.Argument.of("input" + i));
                                }
                                return Collections.singletonList(Signature.of(arguments));
                            }
                        })
                .outputTypeStrategy(callContext -> Optional.of(DataTypes.DOUBLE()))
                .build();
    }

    /**
     * Returns a string representation of this scalar function.
     *
     * @return a string representation of this scalar function
     */
    @Override
    public String toString() {
        return String.format("MLMinMaxScalarFunction {functionName=%s}", functionName);
    }
}
