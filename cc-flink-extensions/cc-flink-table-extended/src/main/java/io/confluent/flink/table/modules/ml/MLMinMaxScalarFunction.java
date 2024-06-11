/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** A scalar function for performing Min-Max scaling on numerical values. */
public class MLMinMaxScalarFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_MIN_MAX_SCALAR";
    /** The name of the function. */
    public final String functionName;
    /** Supported data types for function arguments. */
    public static final DataType[] DATA_TYPE_VALUES =
            new DataType[] {
                DataTypes.DOUBLE(),
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.FLOAT(),
                DataTypes.SMALLINT(),
                DataTypes.NULL()
            };
    /** Set of supported data types for function arguments. */
    public static final Set<DataType> DATA_TYPE_SET =
            new HashSet<>(Arrays.asList(DATA_TYPE_VALUES));

    /**
     * Constructs a new {@code MLMinMaxScalarFunction} instance with the specified function name.
     *
     * @param functionName the name of the function
     */
    public MLMinMaxScalarFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Scales the input value using Min-Max scaling.
     *
     * @param args input arguments (value, dataMin, dataMax)
     * @return the scaled value
     */
    public Double eval(Object... args) {
        if (Objects.isNull(args[0])) {
            return null;
        }
        Object value = args[0];
        Object dataMin = args[1];
        Object dataMax = args[2];
        if (value instanceof Long || dataMin instanceof Long || dataMax instanceof Long) {
            return getStandardizedValue(
                    ((Number) args[0]).longValue(),
                    ((Number) args[1]).longValue(),
                    ((Number) args[2]).longValue());
        }
        if (value instanceof Number && dataMin instanceof Number && dataMax instanceof Number) {
            return getStandardizedValue(
                    ((Number) args[0]).doubleValue(),
                    ((Number) args[1]).doubleValue(),
                    ((Number) args[2]).doubleValue());
        } else {
            throw new FlinkRuntimeException("Invalid Input arguments should be numerical value");
        }
    }

    /**
     * Calculates the standardized value using Min-Max scaling formula for double values.
     *
     * @param value the input value
     * @param dataMin the minimum value of the data
     * @param dataMax the maximum value of the data
     * @return the standardized value
     */
    private Double getStandardizedValue(Double value, Double dataMin, Double dataMax) {
        if (dataMax < dataMin) {
            throw new FlinkRuntimeException(
                    "dataMax value has to be greater than or equal to dataMin");
        }
        double range = dataMax - dataMin;
        range = range == 0.0 ? 1.0 : range;
        return (value - dataMin) / range;
    }

    /**
     * Calculates the standardized value using Min-Max scaling formula for long values.
     *
     * @param value the input value
     * @param dataMin the minimum value of the data
     * @param dataMax the maximum value of the data
     * @return the standardized value
     */
    private Double getStandardizedValue(Long value, Long dataMin, Long dataMax) {
        if (dataMax < dataMin) {
            throw new FlinkRuntimeException(
                    "dataMax value has to be greater than or equal to dataMin");
        }
        long range = dataMax - dataMin;
        range = range == 0L ? 1L : range;
        return (double) (value - dataMin) / range;
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
                                for (int i = 0; i < argsDataTypes.size(); i++) {
                                    DataType argDataType = argsDataTypes.get(i);

                                    // Convert non-nullable data type to nullable if necessary
                                    if (argDataType instanceof AtomicDataType
                                            && !argDataType.getLogicalType().isNullable()) {
                                        argDataType = argDataType.nullable();
                                    }

                                    // Check if the data type is supported
                                    if (!DATA_TYPE_SET.contains(argDataType)
                                            && !argDataType.toString().startsWith("DECIMAL")) {
                                        String errorMessage =
                                                "Unsupported data type: " + argDataType;
                                        if (throwOnFailure) {
                                            throw new ValidationException(errorMessage);
                                        } else {
                                            return Optional
                                                    .empty(); // Return empty Optional if validation
                                            // fails
                                        }
                                    }

                                    // Check if dataMin and dataMax value are null
                                    if (i > 0 && argDataType.equals(DataTypes.NULL())) {
                                        String errorMessage =
                                                "Unsupported data type: dataMin and dataMax value cannot be null";
                                        if (throwOnFailure) {
                                            throw new ValidationException(errorMessage);
                                        } else {
                                            return Optional
                                                    .empty(); // Return empty Optional if validation
                                            // fails
                                        }
                                    }
                                }

                                // If all checks pass, return the list of argument data types
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
