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
        if (args[1] instanceof Long && args[2] instanceof Long) {
            return getStandardizedValue(
                    getLongValue(args[0]), getLongValue(args[1]), getLongValue(args[2]));
        }
        return getStandardizedValue(
                getDoubleValue(args[0]), getDoubleValue(args[1]), getDoubleValue(args[2]));
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

    private Double getDoubleValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return convertToNumberTypedValue(value).doubleValue();
    }

    private Long getLongValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return convertToNumberTypedValue(value).longValue();
    }

    private Number convertToNumberTypedValue(Object value) {
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).getEpochSecond();
        }
        if (value instanceof LocalDate) {
            return ((LocalDate) value).toEpochDay();
        }
        if (value instanceof LocalTime) {
            return ((LocalTime) value).toNanoOfDay();
        }
        if (value instanceof Instant) {
            return ((Instant) value).toEpochMilli();
        }
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toInstant().toEpochMilli();
        }
        if (value instanceof Period) {
            return ((Period) value).getDays();
        }
        if (value instanceof Duration) {
            return ((Duration) value).toMillis();
        } else {
            throw new FlinkRuntimeException(
                    String.format("Unsupported datatype passed as argument to %s function", NAME));
        }
    }
}
