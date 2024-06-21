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

/** Unit tests for the {@link MLMinMaxScalerFunction} class. */
public class MLMinMaxScalerFunctionTest {

    private TableEnvironment tableEnv;

    @BeforeEach
    public void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        tableEnv = TableEnvironment.create(settings);
        // Registering temp function
        final String functionName = "ML_MIN_MAX_SCALER";
        tableEnv.createTemporaryFunction(functionName, new MLMinMaxScalerFunction(functionName));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    void testMLMinMaxScalerForIntValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(2, 1, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.25);
    }

    @Test
    void testMLMinMaxScalerForDoubleValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(3.0, 1, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLMinMaxScalerForLongValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(CAST(5 AS BIGINT), Cast(1 AS BIGINT),"
                                + " Cast(5 AS BIGINT)) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMinMaxScalerForFloatValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(CAST(1 AS FLOAT), Cast(1 AS FLOAT),"
                                + " Cast(5 AS FLOAT)) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMLMinMaxScalerForZeroDataRange() {
        final TableResult mlqueryResult =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(5, 5, 5) AS scaled_value\n;");
        Row row = mlqueryResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMLMinMaxScalerForZeroDataRangeAndDifferentValue() {
        final TableResult mlqueryResult =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(6, 5, 5) AS scaled_value\n;");
        Row row = mlqueryResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMinMaxScalerForNullValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(NULL, 1, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(null);
    }

    @Test
    void testMLMinMaxScalerForDurationValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(INTERVAL '12 18' DAY(2) TO HOUR,"
                                + " INTERVAL '10 18' DAY(2) TO HOUR, INTERVAL '15 18' DAY(2) TO HOUR) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    void testMLMinMaxScalerForLocalDateTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(TIMESTAMP '2024-06-01 12:00:00'"
                                + ", TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-12-31 23:59:59') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.41666667984298095);
    }

    @Test
    void testMLMinMaxscalerForLocalDateValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(DATE '2024-06-01', DATE '2024-01-01'"
                                + ", DATE '2024-12-31') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.41643835616438357);
    }

    @Test
    void testMLMinMaxScalerForLocalTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(TIME '12:00:00', TIME '00:00:00'"
                                + ", TIME '23:59:59') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5000057871040174);
    }

    @Test
    void testMLMinMaxScalerForInstantValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(CAST('2024-06-15 10:00:00' as "
                                + "TIMESTAMP_LTZ), CAST('2024-06-10 10:00:00' as TIMESTAMP_LTZ), CAST"
                                + "('2024-06-20 10:00:00' as TIMESTAMP_LTZ)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLMinMaxScalerForPeriodValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(INTERVAL '11' YEAR, "
                                + "INTERVAL '10' YEAR, INTERVAL '15' YEAR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.2);
    }

    @Test
    void testMLMinMaxScalerForValueLessThanMinData() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(1, 2, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMLMinMaxScalerForValueMoreThanMaxData() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(6, 2, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMinMaxScalerForNaNValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(CAST('NaN' AS DOUBLE), 2, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assert row.getField(0) != null;
        assertThat(((Double) row.getField(0)).isNaN());
    }

    @Test
    void testMLMinMaxScalerForVeryLargeValues() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(CAST(1234567890123456790 AS BIGINT)"
                                + ", CAST(1234567890123456785 AS BIGINT), CAST(1234567890123456795 AS BIGINT)) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assert row.getField(0) != null;
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLMinMaxScalerForNullMinData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALER(1, NULL, 1) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalerForNullMaxData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALER(1, 1, NULL) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalerForStringValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALER('value', 1, 5) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalerForStringMinData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALER(1, 'value', 5) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalerForStringMaxData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALER(1, 1, 'value') AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalerForInvalidDataMinAndDataMax() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(2, 5, 1) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLMinMaxScalerForNaNMinData() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(2, CAST('NaN' AS DOUBLE), 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLMinMaxScalerForNaNMaxData() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALER(2, 1, CAST('NaN' AS DOUBLE)) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLMinMaxScalerForInvalidNumberOfArgumentsMore() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALER(2, 1, 3, 4) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalerForInvalidNumberOfArgumentsLess() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_MIN_MAX_SCALER(2, 1) AS scaled_value\n;"));
    }
}
