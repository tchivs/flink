/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.functions;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

/** Unit tests for the {@link MLMaxAbsScalerFunction} class. */
class MLMaxAbsScalerFunctionTest {

    private TableEnvironment tableEnv;

    @BeforeEach
    public void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        tableEnv = TableEnvironment.create(settings);
        // Registering temp function
        final String functionName = "ML_MAX_ABS_SCALER";
        tableEnv.createTemporaryFunction(functionName, new MLMaxAbsScalerFunction(functionName));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    void testMaxAbsScalerForIntValue() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(1, 5) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.2);
    }

    @Test
    void testMaxAbsScalerForNegativeIntValue() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(-1, 5) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(-0.2);
    }

    @Test
    void testMLMaxAbsScalerForDoubleValue() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(3.0, 5.0) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.6);
    }

    @Test
    void testMLMaxAbsScalerForLongValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(CAST(5 AS BIGINT), CAST(10 AS BIGINT)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLMaxAbsScalerForFloatValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLMaxAbsScalerForNullValue() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(NULL, 5) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(null);
    }

    @Test
    void testMLMaxAbsScalerForDurationValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(INTERVAL '10 00' DAY(2) TO HOUR,"
                                + " INTERVAL '20 00' DAY(2) TO HOUR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLMaxAbsScalerForLocalDateTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(TIMESTAMP '2024-06-01 12:00:00',"
                                + " TIMESTAMP '2024-12-31 23:59:59') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.9893722938648548);
    }

    @Test
    void testMLMaxAbsScalerForLocalDateValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(DATE '2024-06-01', DATE '2024-06-01') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMaxAbsScalerForLocalTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(TIME '12:00:00', TIME '23:59:59') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5000057871040174);
    }

    @Test
    void testMLMaxAbsScalerForInstantValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(CAST('2024-06-20 10:00:00' as "
                                + "TIMESTAMP_LTZ), CAST('2024-06-20 10:00:00' as TIMESTAMP_LTZ)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMaxAbsScalerForPeriodValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(INTERVAL '10' YEAR, "
                                + "INTERVAL '15' YEAR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.6666666666666666);
    }

    @Test
    void testMLMaxAbsScalerForMultiDataTypes() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(120, "
                                + "INTERVAL '10' YEAR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMaxAbsScalerForValueMoreThanMaxData() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(6, 5) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMaxAbsScalerForValueLessThanNegativeMaxData() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(-6, 3) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(-1.0);
    }

    @Test
    void testMaxAbsScalerForZeroAbsMax() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(0, 0) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMaxAbsScalerForZeroAbsMaxAndPositiveValue() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(0.3, 0) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMaxAbsScalerForZeroAbsMaxAndNegativeValue() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(-0.3, 0) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(-1.0);
    }

    @Test
    void testMLMaxAbsScalerForInfinityAbsMax() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(-6, CAST('Infinity' as DOUBLE)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMLMaxAbsScalerForNaNValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(CAST('NaN' AS DOUBLE), 5) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat((Double) row.getField(0)).isNaN();
    }

    @Test
    void testMLMaxAbsScalerForInfValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(CAST('Infinity' AS DOUBLE), 5) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat((Double) row.getField(0)).isInfinite();
    }

    @Test
    void testMLMaxAbsScalerForVeryLargeValues() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(CAST(1234567890123456795 AS BIGINT)"
                                + ", CAST(1234567890123456795 AS BIGINT)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMaxAbsScalerForNullMaxData() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(1, NULL) AS scaled_value\n;"));
    }

    @Test
    void testMLMaxAbsScalerForNaNMaxData() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MAX_ABS_SCALER(2, CAST('NaN' AS DOUBLE)) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLMaxAbsScalerForStringValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MAX_ABS_SCALER('value', 5) AS scaled_value\n;"));
    }

    @Test
    void testMLMaxAbsScalerForStringMaxData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MAX_ABS_SCALER(1, 'value') AS scaled_value\n;"));
    }

    @Test
    void testMLMaxAbsScalerForInvalidNumberOfArgumentsMore() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(2, 1, 3) AS scaled_value\n;"));
    }

    @Test
    void testMLMaxAbsScalerForInvalidNumberOfArgumentsLess() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_MAX_ABS_SCALER(2) AS scaled_value\n;"));
    }
}
