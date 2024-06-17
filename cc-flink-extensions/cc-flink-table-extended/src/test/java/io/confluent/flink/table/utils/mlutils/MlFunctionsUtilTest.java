/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.mlutils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for the {@link MlFunctionsUtil} class. */
class MlFunctionsUtilTest {

    @Test
    void testGetBaseDataTypes() {
        Set<DataType> baseDataTypes = MlFunctionsUtil.getBaseDataTypes();
        assertTrue(baseDataTypes.contains(DataTypes.DOUBLE()));
        assertTrue(baseDataTypes.contains(DataTypes.INT()));
        assertTrue(baseDataTypes.contains(DataTypes.BIGINT()));
        assertTrue(baseDataTypes.contains(DataTypes.FLOAT()));
        assertTrue(baseDataTypes.contains(DataTypes.SMALLINT()));
        assertTrue(baseDataTypes.contains(DataTypes.NULL()));
        assertTrue(baseDataTypes.contains(DataTypes.DATE()));
    }

    @Test
    void testGetDoubleValue_Null() {
        assertNull(MlFunctionsUtil.getDoubleValue(null, "test"));
    }

    @Test
    void testGetDoubleValue_Number() {
        assertEquals(42.0, MlFunctionsUtil.getDoubleValue(42, "test"));
        assertEquals(42.5, MlFunctionsUtil.getDoubleValue(42.5, "test"));
    }

    @Test
    void testGetDoubleValue_UnsupportedType() {
        LocalDateTime now = LocalDateTime.now();
        double expectedValue = now.toInstant(ZoneOffset.UTC).getEpochSecond();
        assertEquals(expectedValue, MlFunctionsUtil.getDoubleValue(now, "test"));
    }

    @Test
    void testGetLongValue_Null() {
        assertNull(MlFunctionsUtil.getLongValue(null, "test"));
    }

    @Test
    void testGetLongValue_Number() {
        assertEquals(42L, MlFunctionsUtil.getLongValue(42, "test"));
        assertEquals(42L, MlFunctionsUtil.getLongValue(42L, "test"));
    }

    @Test
    void testGetLongValue_UnsupportedType() {
        LocalDateTime now = LocalDateTime.now();
        long expectedValue = now.toInstant(ZoneOffset.UTC).getEpochSecond();
        assertEquals(expectedValue, MlFunctionsUtil.getLongValue(now, "test"));
    }

    @Test
    void testConvertToNumberTypedValue_LocalDateTime() {
        LocalDateTime now = LocalDateTime.now();
        long expectedValue = now.toInstant(ZoneOffset.UTC).getEpochSecond();
        assertEquals(expectedValue, MlFunctionsUtil.getLongValue(now, "test"));
    }

    @Test
    void testConvertToNumberTypedValue_LocalDate() {
        LocalDate today = LocalDate.now();
        long expectedValue = today.toEpochDay();
        assertEquals(expectedValue, MlFunctionsUtil.getLongValue(today, "test"));
    }

    @Test
    void testConvertToNumberTypedValue_LocalTime() {
        LocalTime time = LocalTime.now();
        long expectedValue = time.toNanoOfDay();
        assertEquals(expectedValue, MlFunctionsUtil.getLongValue(time, "test"));
    }

    @Test
    void testConvertToNumberTypedValue_Instant() {
        Instant instant = Instant.now();
        long expectedValue = instant.toEpochMilli();
        assertEquals(expectedValue, MlFunctionsUtil.getLongValue(instant, "test"));
    }

    @Test
    void testConvertToNumberTypedValue_ZonedDateTime() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        long expectedValue = zonedDateTime.toInstant().toEpochMilli();
        assertEquals(expectedValue, MlFunctionsUtil.getLongValue(zonedDateTime, "test"));
    }

    @Test
    void testConvertToNumberTypedValue_Period() {
        Period period = Period.ofDays(5);
        int expectedValue = period.getDays();
        assertEquals(expectedValue, MlFunctionsUtil.getLongValue(period, "test"));
    }

    @Test
    void testConvertToNumberTypedValue_Duration() {
        Duration duration = Duration.ofHours(5);
        long expectedValue = duration.toMillis();
        assertEquals(expectedValue, MlFunctionsUtil.getLongValue(duration, "test"));
    }

    @Test
    void testConvertToNumberTypedValue_UnsupportedType_Exception() {
        assertThrows(
                FlinkRuntimeException.class,
                () -> MlFunctionsUtil.getLongValue(new Object(), "test"));
    }
}
