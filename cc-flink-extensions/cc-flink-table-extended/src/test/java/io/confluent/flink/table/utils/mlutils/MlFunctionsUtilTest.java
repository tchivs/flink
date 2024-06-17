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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

/** Unit tests for the {@link MlFunctionsUtil} class. */
class MlFunctionsUtilTest {

    @Test
    void testGetBaseDataTypes() {
        Set<DataType> baseDataTypes = MlFunctionsUtil.getBaseDataTypes();
        assertThat(baseDataTypes.contains(DataTypes.DOUBLE())).isTrue();
        assertThat(baseDataTypes.contains(DataTypes.INT())).isTrue();
        assertThat(baseDataTypes.contains(DataTypes.BIGINT())).isTrue();
        assertThat(baseDataTypes.contains(DataTypes.FLOAT())).isTrue();
        assertThat(baseDataTypes.contains(DataTypes.SMALLINT())).isTrue();
        assertThat(baseDataTypes.contains(DataTypes.NULL())).isTrue();
        assertThat(baseDataTypes.contains(DataTypes.DATE())).isTrue();
    }

    @Test
    void testGetDoubleValue_Null() {
        assertThat(MlFunctionsUtil.getDoubleValue(null, "test")).isNull();
    }

    @Test
    void testGetDoubleValue_Number() {
        assertThat(MlFunctionsUtil.getDoubleValue(42, "test")).isEqualTo(42.0);
        assertThat(MlFunctionsUtil.getDoubleValue(42.5, "test")).isEqualTo(42.5);
    }

    @Test
    void testGetDoubleValue_UnsupportedType() {
        LocalDateTime now = LocalDateTime.now();
        double expectedValue = now.toInstant(ZoneOffset.UTC).getEpochSecond();
        assertThat(MlFunctionsUtil.getDoubleValue(now, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testGetLongValue_Null() {
        assertThat(MlFunctionsUtil.getLongValue(null, "test")).isNull();
    }

    @Test
    void testGetLongValue_Number() {
        assertThat(MlFunctionsUtil.getLongValue(42, "test")).isEqualTo(42L);
        assertThat(MlFunctionsUtil.getLongValue(42L, "test")).isEqualTo(42L);
    }

    @Test
    void testGetLongValue_UnsupportedType() {
        LocalDateTime now = LocalDateTime.now();
        long expectedValue = now.toInstant(ZoneOffset.UTC).getEpochSecond();
        assertThat(MlFunctionsUtil.getLongValue(now, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testConvertToNumberTypedValue_LocalDateTime() {
        LocalDateTime now = LocalDateTime.now();
        long expectedValue = now.toInstant(ZoneOffset.UTC).getEpochSecond();
        assertThat(MlFunctionsUtil.getLongValue(now, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testConvertToNumberTypedValue_LocalDate() {
        LocalDate today = LocalDate.now();
        long expectedValue = today.toEpochDay();
        assertThat(MlFunctionsUtil.getLongValue(today, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testConvertToNumberTypedValue_LocalTime() {
        LocalTime time = LocalTime.now();
        long expectedValue = time.toNanoOfDay();
        assertThat(MlFunctionsUtil.getLongValue(time, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testConvertToNumberTypedValue_Instant() {
        Instant instant = Instant.now();
        long expectedValue = instant.toEpochMilli();
        assertThat(MlFunctionsUtil.getLongValue(instant, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testConvertToNumberTypedValue_ZonedDateTime() {
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        long expectedValue = zonedDateTime.toInstant().toEpochMilli();
        assertThat(MlFunctionsUtil.getLongValue(zonedDateTime, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testConvertToNumberTypedValue_Period() {
        Period period = Period.ofDays(5);
        int expectedValue = period.getDays();
        assertThat(MlFunctionsUtil.getLongValue(period, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testConvertToNumberTypedValue_Duration() {
        Duration duration = Duration.ofHours(5);
        long expectedValue = duration.toMillis();
        assertThat(MlFunctionsUtil.getLongValue(duration, "test")).isEqualTo(expectedValue);
    }

    @Test
    void testConvertToNumberTypedValue_UnsupportedType_Exception() {
        assertThrows(
                FlinkRuntimeException.class,
                () -> MlFunctionsUtil.getLongValue(new Object(), "test"));
    }
}
