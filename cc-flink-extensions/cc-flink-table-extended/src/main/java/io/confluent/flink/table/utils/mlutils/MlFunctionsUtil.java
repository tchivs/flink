/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.mlutils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Utility class for ML functions. Provides methods for type conversion and data type validation.
 */
public class MlFunctionsUtil {

    /**
     * Returns a set of base data types supported by ML functions.
     *
     * @return a set of supported data types.
     */
    public static Set<DataType> getBaseDataTypes() {
        final DataType[] dataTypesValues =
                new DataType[] {
                    DataTypes.DOUBLE(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.SMALLINT(),
                    DataTypes.NULL(),
                    DataTypes.DATE()
                };
        /** Set of base data types for function arguments. */
        return new HashSet<>(Arrays.asList(dataTypesValues));
    }

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
            return ((Period) value).getDays();
        }
        if (value instanceof Duration) {
            return ((Duration) value).toMillis();
        } else {
            throw new FlinkRuntimeException(
                    String.format("Unsupported datatype passed as argument to %s function", name));
        }
    }
}
