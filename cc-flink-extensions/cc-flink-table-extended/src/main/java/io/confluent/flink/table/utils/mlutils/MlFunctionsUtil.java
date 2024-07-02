/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.mlutils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Utility class for ML functions. Provides methods for type conversion and data type validation.
 */
public class MlFunctionsUtil {

    /**
     * Converts the given value to a Double.
     *
     * @param value the value to be converted.
     * @param name the name of the function (for error messages).
     * @return the converted value as a Double.
     */
    public static Double getDoubleValue(Object value, String name) {
        if (Objects.isNull(value)) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return convertToNumberTypedValue(value, name).doubleValue();
    }

    /**
     * Converts the given value to a Long.
     *
     * @param value the value to be converted.
     * @param name the name of the function (for error messages).
     * @return the converted value as a Long.
     */
    public static Long getLongValue(Object value, String name) {
        if (Objects.isNull(value)) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return convertToNumberTypedValue(value, name).longValue();
    }

    /**
     * Converts the given value to a Number.
     *
     * @param value the value to be converted.
     * @param name the name of the function (for error messages).
     * @return the converted value as a Number.
     * @throws FlinkRuntimeException if the value type is not supported.
     */
    private static Number convertToNumberTypedValue(Object value, String name) {
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
            return ((Period) value).toTotalMonths();
        }
        if (value instanceof Duration) {
            return ((Duration) value).toMillis();
        } else {
            throw new FlinkRuntimeException(
                    String.format("Unsupported datatype passed as argument to %s function", name));
        }
    }

    /**
     * Creates a TypeInference object for scaler functions with configurable input and output type
     * strategies.
     *
     * @param minCount The minimum number of arguments expected.
     * @param maxCount The maximum number of arguments expected.
     * @param outputDataType The DataType of the output.
     * @param isValidCount A function that determines if the given argument count is valid.
     * @param validateInputTypes A function that validates the input types and returns an error
     *     message if invalid.
     * @return A TypeInference object with the specified input and output type strategies.
     */
    public static TypeInference getTypeInferenceForMLScalarFunctions(
            Integer minCount,
            Integer maxCount,
            DataType outputDataType,
            Function<Integer, Boolean> isValidCount,
            Function<List<DataType>, Optional<String>> validateInputTypes) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        new InputTypeStrategy() {
                            @Override
                            public ArgumentCount getArgumentCount() {
                                return new ArgumentCount() {

                                    @Override
                                    public boolean isValidCount(int count) {
                                        return isValidCount.apply(count);
                                    }

                                    @Override
                                    public Optional<Integer> getMinCount() {
                                        return Optional.of(minCount);
                                    }

                                    @Override
                                    public Optional<Integer> getMaxCount() {
                                        return Optional.of(maxCount);
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
                                        validateInputTypes.apply(argumentDataTypes);

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
                                for (int i = 0; i < maxCount; i++) {
                                    arguments.add(Signature.Argument.of("input" + i));
                                }
                                return Collections.singletonList(Signature.of(arguments));
                            }
                        })
                .outputTypeStrategy(callContext -> Optional.of(outputDataType))
                .build();
    }
}
