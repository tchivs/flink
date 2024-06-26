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

/** Unit tests for the {@link MLRobustScalerFunction} class. */
class MLRobustScalerFunctionTest {

    private TableEnvironment tableEnv;

    @BeforeEach
    void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        tableEnv = TableEnvironment.create(settings);
        // Registering temp function
        final String functionName = "ML_ROBUST_SCALER";
        tableEnv.createTemporaryFunction(functionName, new MLRobustScalerFunction(functionName));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    void testRobustScalerFunctionForIntValues() {
        TableResult result =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(4, 2, 1, 3) as scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    public void testRobustScalerForIntValuesWithBoolean() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, 1, 0, 3, TRUE, TRUE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.3333333333333333);
    }

    @Test
    public void testRobustScalerForIntValuesWithFalseBooleans() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, 1, 0, 5, FALSE, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(2.0);
    }

    @Test
    public void testRobustScalerForIntValuesWithCentering() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, 1, 5, 3, TRUE, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    public void testRobustScalerForIntValuesWithCenteringAndNULLIQR() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, 1, NULL, NULL, TRUE, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    public void testRobustScalerForIntValuesWithCenteringAndNaNIQR() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, 1, CAST('NaN' as DOUBLE), CAST('NaN' as DOUBLE), TRUE, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    public void testRobustScalerForIntValuesWithScaling() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, 1, 2, 4, FALSE, TRUE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    public void testRobustScalerForIntValuesWithCenteringFalse() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, 1, 0, 5, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    public void testRobustScalerForIntValuesWithScalingAndNULLMedian() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, NULL, 0, 5, FALSE, TRUE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    public void testRobustScalerForIntValuesWithScalingAndNaNMedian() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, CAST('NaN' as DOUBLE), 0, 5, FALSE, TRUE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    void testMLRobustScalerForDoubleValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(3.0, 1.0, 0.0, 5.0) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    void testMLRobustScalerForLongValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(5 AS BIGINT), CAST(10 AS BIGINT), CAST(5 AS BIGINT), CAST(15 AS BIGINT)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(-0.5);
    }

    @Test
    void testMLRobustScalerForLongValueAndDoubleQuartile() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(15 AS BIGINT), CAST(10 AS BIGINT), 5.0, 15.0) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLRobustScalerForFloatValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT), CAST(-2.0 AS FLOAT), CAST(2.0 AS FLOAT)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.25);
    }

    @Test
    void testMLRobustScalerForNullValue() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(NULL, 1, 5, 6) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(null);
    }

    @Test
    void testMLRobustScalerForNaNValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST('NaN' AS DOUBLE), 1, 5, 6) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(Double.NaN);
    }

    @Test
    void testMLRobustScalerForInfValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST('+Infinity' AS DOUBLE), 1, 5, 6) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void testMLRobustScalerForDurationValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(INTERVAL '10 00' DAY(2) TO HOUR,"
                                + " INTERVAL '08 00' DAY(2) TO HOUR,"
                                + " INTERVAL '00 00' DAY(2) TO HOUR, INTERVAL '20 00' DAY(2) TO HOUR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.1);
    }

    @Test
    void testMLRobustScalerForLocalDateTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(TIMESTAMP '2024-06-01 00:05:00',"
                                + " TIMESTAMP '2024-06-01 00:10:00',"
                                + " TIMESTAMP '2024-06-01 00:05:00', TIMESTAMP '2024-06-01 00:15:00') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(-0.5);
    }

    @Test
    void testMLRobustScalerForLocalDateValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(DATE '2024-06-06', DATE '2024-06-03', DATE '2024-06-01', DATE '2024-06-10') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.3333333333333333);
    }

    @Test
    void testMLRobustScalerForLocalTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(TIME '12:00:00', TIME '14:00:00', TIME '10:30:00', TIME '15:30:00') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(-0.4);
    }

    @Test
    void testMLRobustScalerForInstantValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST('2024-06-01 10:00:00' as "
                                + "TIMESTAMP_LTZ), CAST('2024-06-01 05:00:00' as TIMESTAMP_LTZ),"
                                + " CAST('2024-06-01 00:00:00' as TIMESTAMP_LTZ), CAST('2024-06-01 20:00:00' as TIMESTAMP_LTZ)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.25);
    }

    @Test
    void testMLRobustScalerForPeriodValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(INTERVAL '10' YEAR, "
                                + "INTERVAL '5' YEAR, INTERVAL '0' YEAR,  INTERVAL '15' YEAR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.3333333333333333);
    }

    @Test
    void testMLRobustScalerForMultiDataTypes() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(180, 120, "
                                + "INTERVAL '10' YEAR, INTERVAL '20' YEAR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLRobustScalerForDoubleValueAndIQR() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(1, 5, 5, 5) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(-4.0);
    }

    @Test
    void testMLRobustScalerForLongValueAndZeroIQR() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(1 as BIGINT), CAST(2 as BIGINT), 2.0, 2.0) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(-1.0);
    }

    @Test
    void testMLRobustScalerForVeryLargeValues() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(1234567890123456795 AS BIGINT),"
                                + " CAST(1234567890123456780 AS BIGINT), CAST(1234567890123456780 AS BIGINT),"
                                + " CAST(1234567890123456795 AS BIGINT) ) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLRobustScalerForInfinity() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(-6, 5, CAST('-Infinity' as DOUBLE), CAST('-Infinity' as DOUBLE)) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForNullFirstQuartile() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(1, 0, NULL, 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForNullThirdQuartile() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(1, 0, 5, NULL) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForNaNIQR() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, 0, CAST('NaN' AS DOUBLE), 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForNullMedian() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(1, NULL, 0, 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForNaNMedian() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(2, CAST('NaN' AS DOUBLE), 0, 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForLessThanIQRMedian() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(2, -1, 0, 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForMoreThanIQRMedian() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(2, 6, 0, 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForMoreThanIQRMedianLongValues() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(2 as BIGINT), CAST(6 as BIGINT), CAST(2 as BIGINT), CAST(5 as BIGINT)) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerFoInvalidIQRValues() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_ROBUST_SCALER(2, 6, 7, 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerFoInvalidIQRValuesLong() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(2 as BIGINT), CAST(2 as BIGINT), CAST(7 as BIGINT), CAST(5 as BIGINT)) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForNUllMedianValueLong() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(6 as BIGINT), CAST(NULL as BIGINT), CAST(5 as BIGINT), CAST(7 as BIGINT)) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForNUllIQRValuesLong() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_ROBUST_SCALER(CAST(6 as BIGINT), CAST(NULL as BIGINT), CAST(NULL as BIGINT), CAST(NULL as BIGINT)) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLRobustScalerForStringValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_ROBUST_SCALER('value', 0, 5, 7) AS scaled_value\n;"));
    }

    @Test
    void testMLRobustScalerForStringIQR() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_ROBUST_SCALER(1, 0, 'value', 6) AS scaled_value\n;"));
    }

    @Test
    void testMLRobustScalerForStringMedian() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_ROBUST_SCALER(1, 'value', 0, 7) AS scaled_value\n;"));
    }

    @Test
    void testMLRobustScalerForInvalidNumberOfArgumentsMore() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_ROBUST_SCALER(2, 1, 3, 4, TRUE, TRUE, TRUE) AS scaled_value\n;"));
    }

    @Test
    void testMLRobustScalerForInvalidNumberOfArgumentsLess() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_ROBUST_SCALER(2, 1) AS scaled_value\n;"));
    }

    @Test
    void testMLRobustScalerForInvalidDataTypeForWithCentering() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_ROBUST_SCALER(2, 3, 2, 5, 4) AS scaled_value\n;"));
    }

    @Test
    void testMLRobustScalerForInvalidDataTypeForWithScaling() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_ROBUST_SCALER(2, 3, 2, 4, TRUE,5.0) AS scaled_value\n;"));
    }
}
