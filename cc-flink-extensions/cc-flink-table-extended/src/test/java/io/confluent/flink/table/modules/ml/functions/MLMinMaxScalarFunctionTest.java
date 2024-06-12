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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

public class MLMinMaxScalarFunctionTest {

    private TableEnvironment tableEnv;

    @BeforeEach
    public void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        tableEnv = TableEnvironment.create(settings);
        // Registering temp function
        final String functionName = "ML_MIN_MAX_SCALAR";
        tableEnv.createTemporaryFunction(functionName, new MLMinMaxScalarFunction(functionName));
    }

    @Test
    void testMLMinMaxScalarForIntValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALAR(2, 1, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.25);
    }

    @Test
    void testMLMinMaxScalarForDoubleValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALAR(3.0, 1, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLMinMaxScalarForLongValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(CAST(5 AS BIGINT), Cast(1 AS BIGINT),"
                                + " Cast(5 AS BIGINT)) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLMinMaxScalarForFloatValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(CAST(1 AS FLOAT), Cast(1 AS FLOAT),"
                                + " Cast(5 AS FLOAT)) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMLMinMaxScalarForZeroDataRange() {
        final TableResult mlqueryResult =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALAR(5, 5, 5) AS scaled_value\n;");
        Row row = mlqueryResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMLMinMaxScalarForNullValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_MIN_MAX_SCALAR(NULL, 1, 5) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(null);
    }

    @Test
    void testMLMinMaxScalarForDurationValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(INTERVAL '12 18' DAY(2) TO HOUR,"
                                + " INTERVAL '10 18' DAY(2) TO HOUR, INTERVAL '15 18' DAY(2) TO HOUR) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.4);
    }

    @Test
    void testMLMinMaxScalarForLocalDateTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(TIMESTAMP '2024-06-01 12:00:00'"
                                + ", TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-12-31 23:59:59') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.41666667984298095);
    }

    @Test
    void testMLMinMaxScalarForLocalDateValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(DATE '2024-06-01', DATE '2024-01-01'"
                                + ", DATE '2024-12-31') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.41643835616438357);
    }

    @Test
    void testMLMinMaxScalarForLocalTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(TIME '12:00:00', TIME '00:00:00'"
                                + ", TIME '23:59:59') AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5000057871040174);
    }

    @Test
    void testMLMinMaxScalarForInstantValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(CAST('2024-06-15 10:00:00' as "
                                + "TIMESTAMP_LTZ), CAST('2024-06-10 10:00:00' as TIMESTAMP_LTZ), CAST"
                                + "('2024-06-20 10:00:00' as TIMESTAMP_LTZ)) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLMinMaxScalarForPeriodValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(INTERVAL '12 18' DAY(2) TO HOUR, "
                                + "INTERVAL '10 18' DAY(2) TO HOUR, INTERVAL '20 18' DAY(2) TO HOUR) AS scaled_value\n;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.2);
    }

    @Test
    void testMLMinMaxScalarForNullMinData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALAR(1, NULL, 1) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalarForNullMaxData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALAR(1, 1, NULL) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalarForStringValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALAR('value', 1, 5) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalarForStringMinData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALAR(1, 'value', 5) AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalarForStringMaxData() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALAR(1, 1, 'value') AS scaled_value\n;"));
    }

    @Test
    void testMLMinMaxScalarForDifferentInputValues() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(INTERVAL '12 18' DAY(2) TO HOUR, 2, 1000) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLMinMaxScalarForInvalidDataMinAndDataMax() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(2, 5, 1) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLMinMaxScalarForInvalidValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_MIN_MAX_SCALAR(1, 2, 5) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }
}
