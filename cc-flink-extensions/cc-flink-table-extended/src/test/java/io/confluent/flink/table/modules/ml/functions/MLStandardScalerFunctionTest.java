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
import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for the {@link MLStandardScalerFunction} class. */
public class MLStandardScalerFunctionTest {

    private TableEnvironment tableEnv;

    @BeforeEach
    void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        tableEnv = TableEnvironment.create(settings);
        // Registering temp function
        final String functionName = "ML_STANDARD_SCALER";
        tableEnv.createTemporaryFunction(functionName, new MLStandardScalerFunction(functionName));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    public void testMinMaxScalerForIntValues() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_STANDARD_SCALER(2, 1, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.2);
    }

    @Test
    public void testMinMaxScalerForIntValuesWithBoolean() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(2, 1, 5, TRUE, TRUE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.2);
    }

    @Test
    public void testMinMaxScalerForIntValuesWithFalseBooleans() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(2, 1, 5, FALSE, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(2.0);
    }

    @Test
    public void testMinMaxScalerForIntValuesWithCentering() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(2, 1, 5, TRUE, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    public void testMinMaxScalerForIntValuesWithCenteringAndNULLStdDev() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(2, 1, NULL, TRUE, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    public void testMinMaxScalerForIntValuesWithScaling() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(2, 1, 5, FALSE, TRUE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    public void testMinMaxScalerForIntValuesFalseWithCentering() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_STANDARD_SCALER(2, 1, 5, FALSE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    public void testMinMaxScalerForIntValuesWithScalingAndNULLMean() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(2, NULL, 5, FALSE, TRUE) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    void testMLStandardScalerForDoubleValue() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_STANDARD_SCALER(3.0, 1.0, 5.0) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    void testMLStandardScalerForLongValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(CAST(5 AS BIGINT), CAST(10 AS BIGINT), CAST(5 AS BIGINT)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(-1.0);
    }

    @Test
    void testMLStandardScalerForLongValueAndDoubleStandardDeviation() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(CAST(5 AS BIGINT), CAST(10 AS BIGINT), 5.0) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(-1.0);
    }

    @Test
    void testMLStandardScalerForFloatValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(CAST(1.0 AS FLOAT), CAST(0.0 AS FLOAT), CAST(2.0 AS FLOAT)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLStandardScalerForNullValue() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_STANDARD_SCALER(NULL, 1, 5) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(null);
    }

    @Test
    void testMLStandardScalerForDurationValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(INTERVAL '10 00' DAY(2) TO HOUR,"
                                + " INTERVAL '08 00' DAY(2) TO HOUR,"
                                + " INTERVAL '20 00' DAY(2) TO HOUR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.1);
    }

    @Test
    void testMLStandardScalerForLocalDateTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(TIMESTAMP '2024-06-01 00:00:00',"
                                + " TIMESTAMP '2024-06-01 00:10:00',"
                                + " 2000) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(-0.3);
    }

    @Test
    void testMLStandardScalerForLocalDateValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(DATE '2024-06-06', DATE '2024-06-01', 10) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLStandardScalerForLocalTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(TIME '12:00:00', TIME '14:00:00', TIME '00:30:00') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(-4.0);
    }

    @Test
    void testMLStandardScalerForInstantValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(CAST('2024-06-01 10:00:00' as "
                                + "TIMESTAMP_LTZ), CAST('2024-06-01 00:00:00' as TIMESTAMP_LTZ),"
                                + " 1000000) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(36.0);
    }

    @Test
    void testMLStandardScalerForPeriodValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(INTERVAL '10' YEAR, "
                                + "INTERVAL '5' YEAR, INTERVAL '5' YEAR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLStandardScalerForMultiDataTypes() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(120, 0, "
                                + "INTERVAL '10' YEAR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLStandardScalerForDoubleValueAndZeroStandardDeviation() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_STANDARD_SCALER(1, 2, 0) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(-1.0);
    }

    @Test
    void testMLStandardScalerForLongValueAndZeroStandardDeviation() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(CAST(1 as BIGINT), CAST(2 as BIGINT), 0) as scaled_value;\n");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(-1.0);
    }

    @Test
    void testMLStandardScalerForInfinityStandardDeviation() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(-6, 5,CAST('-Infinity' as DOUBLE)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMLStandardScalerForNaNValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(CAST('NaN' AS DOUBLE), 1, 5) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat((Double) row.getField(0)).isNaN();
    }

    @Test
    void testMLStandardScalerForInfValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(CAST('Infinity' AS DOUBLE), 1, 5) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat((Double) row.getField(0)).isInfinite();
    }

    @Test
    void testMLStandardScalerForVeryLargeValues() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(CAST(1234567890123456795 AS BIGINT),"
                                + " CAST(1234567890123456780 AS BIGINT), 5) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(3.0);
    }

    @Test
    void testMLStandardScalerForNullStandardDeviation() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_STANDARD_SCALER(1, 0, NULL) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLStandardScalerForNaNStandardDeviation() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(2, 0, CAST('NaN' AS DOUBLE)) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLStandardScalerForNullMean() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_STANDARD_SCALER(1, NULL, 0) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLStandardScalerForNaNMean() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_STANDARD_SCALER(2, CAST('NaN' AS DOUBLE), 0) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLStandardScalerForStringValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_STANDARD_SCALER('value', 0, 5) AS scaled_value\n;"));
    }

    @Test
    void testMLStandardScalerForStringStandardDeviation() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_STANDARD_SCALER(1, 0, 'value') AS scaled_value\n;"));
    }

    @Test
    void testMLStandardScalerForStringMean() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_STANDARD_SCALER(1, 'value', 0) AS scaled_value\n;"));
    }

    @Test
    void testMLStandardScalerForInvalidNumberOfArgumentsMore() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_STANDARD_SCALER(2, 1, 3, 4, 5, 6) AS scaled_value\n;"));
    }

    @Test
    void testMLStandardScalerForInvalidNumberOfArgumentsLess() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_STANDARD_SCALER(2, 1) AS scaled_value\n;"));
    }

    @Test
    void testMLStandardScalerForInvalidDataTypeForWithCentering() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_STANDARD_SCALER(2, 1, 2, 4) AS scaled_value\n;"));
    }

    @Test
    void testMLStandardScalerForInvalidDataTypeForWithScaling() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_STANDARD_SCALER(2, 1, 2, 4, 5.0) AS scaled_value\n;"));
    }
}
