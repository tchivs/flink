/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.functions;

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

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

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
                DataTypes.NULL(),
                DataTypes.DATE()
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
        Object dataMin = args[1];
        Object dataMax = args[2];
        if (value instanceof Number && dataMin instanceof Number && dataMax instanceof Number) {
            if (value instanceof Long || dataMin instanceof Long || dataMax instanceof Long) {
                return getStandardizedValue(
                        ((Number) args[0]).longValue(),
                        ((Number) args[1]).longValue(),
                        ((Number) args[2]).longValue());
            }
            return getStandardizedValue(
                    ((Number) args[0]).doubleValue(),
                    ((Number) args[1]).doubleValue(),
                    ((Number) args[2]).doubleValue());
        } else if (value instanceof LocalDateTime
                && dataMin instanceof LocalDateTime
                && dataMax instanceof LocalDateTime) {
            return getStandardizedValue(
                    ((LocalDateTime) args[0]).toInstant(ZoneOffset.UTC).getEpochSecond(),
                    ((LocalDateTime) args[1]).toInstant(ZoneOffset.UTC).getEpochSecond(),
                    ((LocalDateTime) args[2]).toInstant(ZoneOffset.UTC).getEpochSecond());
        } else if (value instanceof LocalDate
                && dataMin instanceof LocalDate
                && dataMax instanceof LocalDate) {
            return getStandardizedValue(
                    ((LocalDate) args[0]).toEpochDay(),
                    ((LocalDate) args[1]).toEpochDay(),
                    ((LocalDate) args[2]).toEpochDay());
        } else if (value instanceof LocalTime
                && dataMin instanceof LocalTime
                && dataMax instanceof LocalTime) {
            return getStandardizedValue(
                    ((LocalTime) args[0]).toNanoOfDay(),
                    ((LocalTime) args[1]).toNanoOfDay(),
                    ((LocalTime) args[2]).toNanoOfDay());
        } else if (value instanceof Instant
                && dataMin instanceof Instant
                && dataMax instanceof Instant) {
            return getStandardizedValue(
                    ((Instant) args[0]).toEpochMilli(),
                    ((Instant) args[1]).toEpochMilli(),
                    ((Instant) args[2]).toEpochMilli());
        } else if (value instanceof ZonedDateTime
                && dataMin instanceof ZonedDateTime
                && dataMax instanceof ZonedDateTime) {
            return getStandardizedValue(
                    ((ZonedDateTime) args[0]).toInstant().toEpochMilli(),
                    ((ZonedDateTime) args[1]).toInstant().toEpochMilli(),
                    ((ZonedDateTime) args[2]).toInstant().toEpochMilli());
        } else if (value instanceof Period
                && dataMin instanceof Period
                && dataMax instanceof Period) {
            return getStandardizedValue(
                    ((Number) ((Period) args[0]).getDays()).doubleValue(),
                    ((Number) ((Period) args[1]).getDays()).doubleValue(),
                    ((Number) ((Period) args[2]).getDays()).doubleValue());
        } else if (value instanceof Duration
                && dataMin instanceof Duration
                && dataMax instanceof Duration) {
            return getStandardizedValue(
                    ((Duration) args[0]).toMillis(),
                    ((Duration) args[1]).toMillis(),
                    ((Duration) args[2]).toMillis());
        } else {
            throw new FlinkRuntimeException(
                    "Invalid Inputs: arguments should either be numerical value or Time value");
        }
    }

    /**
     * Calculates the standardized value using the Min-Max scaling formula for double values.
     *
     * @param value the input value
     * @param dataMin the minimum value of the data
     * @param dataMax the maximum value of the data
     * @return the standardized value
     */
    private Double getStandardizedValue(Double value, Double dataMin, Double dataMax) {
        if (dataMax < dataMin || value<dataMin || value>dataMax) {
            throw new ValidationException(
                    "dataMax value has to be greater than or equal to dataMin and Value has to be between dataMin and DataMax inclusive");
        }
        double range = dataMax - dataMin;
        range = range == 0.0 ? 1.0 : range;
        return (value - dataMin) / range;
    }

    /**
     * Calculates the standardized value using the Min-Max scaling formula for long values.
     *
     * @param value the input value
     * @param dataMin the minimum value of the data
     * @param dataMax the maximum value of the data
     * @return the standardized value
     */
    private Double getStandardizedValue(Long value, Long dataMin, Long dataMax) {
        if (dataMax < dataMin) {
            throw new ValidationException(
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

                                // Use IntStream.range to create indices for elements in
                                // argsDataTypes list
                                Optional<String> errorMessage =
                                        IntStream.range(0, argsDataTypes.size())
                                                .mapToObj(
                                                        i -> {
                                                            DataType argDataType =
                                                                    argsDataTypes.get(i);

                                                            // Convert non-nullable data type to
                                                            // nullable if necessary
                                                            if (argDataType
                                                                            instanceof
                                                                            AtomicDataType
                                                                    && !argDataType
                                                                            .getLogicalType()
                                                                            .isNullable()) {
                                                                argDataType =
                                                                        argDataType.nullable();
                                                            }

                                                            // Check if the data type is supported
                                                            if (!DATA_TYPE_SET.contains(argDataType)
                                                                    && !argDataType
                                                                            .toString()
                                                                            .startsWith("DECIMAL")
                                                                    && !argDataType
                                                                            .toString()
                                                                            .startsWith("TIME")
                                                                    && !argDataType
                                                                            .toString()
                                                                            .startsWith(
                                                                                    "INTERVAL")) {
                                                                return "Unsupported data type: "
                                                                        + argDataType;
                                                            }

                                                            // Check if dataMin and dataMax value
                                                            // are null
                                                            if (i > 0
                                                                    && argDataType.equals(
                                                                            DataTypes.NULL())) {
                                                                return "Unsupported data type: dataMin and dataMax value cannot be null";
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
