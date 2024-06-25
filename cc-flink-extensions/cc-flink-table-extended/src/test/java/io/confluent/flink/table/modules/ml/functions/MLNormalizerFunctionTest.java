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

/** Unit tests for the {@link MLNormalizerFunction} class. */
class MLNormalizerFunctionTest {

    private TableEnvironment tableEnv;

    @BeforeEach
    public void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        tableEnv = TableEnvironment.create(settings);
        // Registering temp function
        final String functionName = "ML_NORMALIZER";
        tableEnv.createTemporaryFunction(functionName, new MLNormalizerFunction(functionName));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    void testNormalizerForIntValue() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_NORMALIZER(1, 5) as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.2);
    }

    @Test
    void testNormalizerForNegativeIntValue() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_NORMALIZER(-1, 5) as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(-0.2);
    }

    @Test
    void testMLNormalizerForDoubleValue() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_NORMALIZER(3.0, 5.0) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.6);
    }

    @Test
    void testMLNormalizerForLongValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(CAST(5 AS BIGINT), CAST(10 AS BIGINT)) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLNormalizerForFloatValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(CAST(1.0 AS FLOAT), CAST(2.0 AS FLOAT)) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLNormalizerForNullValue() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_NORMALIZER(NULL, 5) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(null);
    }

    @Test
    void testMLNormalizerForDurationValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(INTERVAL '10 00' DAY(2) TO HOUR,"
                                + " INTERVAL '20 00' DAY(2) TO HOUR) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5);
    }

    @Test
    void testMLNormalizerForLocalDateTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(TIMESTAMP '2024-06-01 12:00:00',"
                                + " TIMESTAMP '2024-12-31 23:59:59') AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.9893722938648548);
    }

    @Test
    void testMLNormalizerForLocalDateValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(DATE '2024-06-01', DATE '2024-06-01') AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLNormalizerForLocalTimeValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(TIME '12:00:00', TIME '23:59:59') AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.5000057871040174);
    }

    @Test
    void testMLNormalizerForInstantValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(CAST('2024-06-20 10:00:00' as "
                                + "TIMESTAMP_LTZ), CAST('2024-06-20 10:00:00' as TIMESTAMP_LTZ)) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLNormalizerForPeriodValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(INTERVAL '10' YEAR, "
                                + "INTERVAL '15' YEAR) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.6666666666666666);
    }

    @Test
    void testMLNormalizerForMultiDataTypes() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(120, INTERVAL '10' YEAR) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testNormalizerForZeroAbsMax() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_NORMALIZER(3, 0) as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(3.0);
    }

    @Test
    void testMLNormalizerForInfinityAbsMax() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(-6, CAST('Infinity' as DOUBLE)) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.0);
    }

    @Test
    void testMLNormalizerForNaNValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(CAST('NaN' AS DOUBLE), 5) AS scaled_value;");
        Row row = result.collect().next();
        assertThat((Double) row.getField(0)).isNaN();
    }

    @Test
    void testMLNormalizerForInfValue() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(CAST('Infinity' AS DOUBLE), 5) AS scaled_value;");
        Row row = result.collect().next();
        assertThat((Double) row.getField(0)).isInfinite();
    }

    @Test
    void testMLNormalizerForVeryLargeValues() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(CAST(1234567890123456795 AS BIGINT)"
                                + ", CAST(1234567890123456795 AS BIGINT)) AS scaled_value;");
        Row row = result.collect().next();
        assertThat(row.getField(0)).isEqualTo(1.0);
    }

    @Test
    void testMLNormalizerForNullMaxData() {
        Exception e =
                assertThrows(
                        ValidationException.class,
                        () ->
                                tableEnv.executeSql(
                                        "SELECT ML_NORMALIZER(1, NULL) AS scaled_value;"));
        String message = "Illegal use of 'NULL'";
        assertThat(e.getMessage()).contains(message);
    }

    @Test
    void testMLNormalizerForNaNMaxData() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_NORMALIZER(2, CAST('NaN' AS DOUBLE)) AS scaled_value;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLNormalizerForStringValue() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_NORMALIZER('value', 5) AS scaled_value;"));
    }

    @Test
    void testMLNormalizerForStringMaxData() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_NORMALIZER(1, 'value') AS scaled_value;"));
    }

    @Test
    void testMLNormalizerForInvalidNumberOfArgumentsMore() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_NORMALIZER(2, 1, 3) AS scaled_value;"));
    }

    @Test
    void testMLNormalizerForInvalidNumberOfArgumentsLess() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_NORMALIZER(2) AS scaled_value;"));
    }
}
