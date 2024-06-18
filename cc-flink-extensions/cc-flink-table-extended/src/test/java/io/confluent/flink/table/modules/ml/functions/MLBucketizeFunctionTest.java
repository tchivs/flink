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

/** Unit tests for the {@link MLBucketizeFunction} class. */
public class MLBucketizeFunctionTest {

    private TableEnvironment tableEnv;

    @BeforeEach
    public void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        tableEnv = TableEnvironment.create(settings);
        // Registering temp function
        final String functionName = "ML_BUCKETIZE";
        tableEnv.createTemporaryFunction(functionName, new MLBucketizeFunction(functionName));
    }

    @Test
    void testBucketizeForIntValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_BUCKETIZE(2, ARRAY[1, 4, 7]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_2");
    }

    @Test
    void testBucketizeWithBucketNames() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(2, ARRAY[1, 4, 7], ARRAY['b_null','b1','b2','b3','b4']) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("b2");
    }

    @Test
    void testBucketizeWithIntegerBucketNames() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(2, ARRAY[1, 4, 7], ARRAY[null,1,2,3,4]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("2");
    }

    @Test
    void testBucketizeWithTimeStampBucketNames() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(2, ARRAY[1, 7], ARRAY[null,TIMESTAMP '2024-06-01 12:00:01', TIMESTAMP '2024-06-01 12:10:01', TIMESTAMP '2024-06-01 12:20:01']) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("2024-06-01T12:10:01");
    }

    @Test
    void testBucketizeWithIntervalBucketNames() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(2, ARRAY[1], ARRAY[INTERVAL '12 18' DAY(2) TO HOUR, INTERVAL '12 18' DAY(2) TO HOUR, INTERVAL '12 21' DAY(2) TO HOUR]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("PT309H");
    }

    @Test
    void testBucketizeWithInstantBucketNames() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(2, ARRAY[1], ARRAY[CAST(null as TIMESTAMP_LTZ), CAST('2024-06-15 10:00:20' as TIMESTAMP_LTZ), CAST('2024-06-15 10:00:30' as TIMESTAMP_LTZ)]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("2024-06-15T15:00:30Z");
    }

    @Test
    void testBucketizeWithLongBucketNames() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(2, ARRAY[1], ARRAY[CAST(null AS BIGINT), CAST(1234567890123456799 AS BIGINT), CAST(1234567890123456799 AS BIGINT)]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("1234567890123456799");
    }

    @Test
    void testBucketizeWithDoubleBucketNames() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(0, ARRAY[1], ARRAY[CAST('NaN' AS DOUBLE), CAST(0.99 AS DOUBLE), CAST(1.0 AS DOUBLE)]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("0.99");
    }

    @Test
    void testBucketizeWithBucketNamesWithNull() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(0, ARRAY[1, 4, 7], ARRAY['b_null',null,'b2','b3','b4']) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(null);
    }

    @Test
    void testBucketizeForBoundaryValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_BUCKETIZE(4, ARRAY[1, 4, 7]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_3");
    }

    @Test
    void testBucketizeForNullValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_BUCKETIZE(NULL, ARRAY[1, 4, 7]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_NULL");
    }

    @Test
    void testBucketizeForNullValueWithBucketNames() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(NULL, ARRAY[1, 4, 7], ARRAY['b_null','b1','b2','b3','b4']) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("b_null");
    }

    @Test
    void testBucketizeForDoubleValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_BUCKETIZE(1.4, ARRAY[1, 1.2]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_3");
    }

    @Test
    void testBucketizeForDoublePositiveInfiniteValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(CAST('Infinity' AS DOUBLE), ARRAY[1, 1000]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_3");
    }

    @Test
    void testBucketizeForDoubleNegativeInfiniteValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(CAST('-Infinity' AS DOUBLE), ARRAY[1, 1000]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_1");
    }

    @Test
    void testBucketizeForLongValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(CAST('15' AS BIGINT), ARRAY[CAST('1' AS BIGINT), CAST('12' AS BIGINT)]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_3");
    }

    @Test
    void testBucketizeForFloatValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(CAST('15' AS FLOAT), ARRAY[CAST('1' AS FLOAT), CAST('12' AS FLOAT)]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_3");
    }

    @Test
    void testBucketizeForDuration() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(INTERVAL '12 18' DAY(2) TO HOUR, ARRAY[INTERVAL '12 18' DAY(2) TO HOUR, INTERVAL '12 21' DAY(2) TO HOUR]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_2");
    }

    @Test
    void testBucketizeForLocalDateTimeValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(TIMESTAMP '2024-05-01 12:00:00', ARRAY[TIMESTAMP '2024-06-01 12:00:00', TIMESTAMP '2024-06-02 12:00:00']) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_1");
    }

    @Test
    void testBucketizeForLocalDateValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(DATE '2024-06-02', ARRAY[DATE '2024-06-01', DATE '2024-06-03']) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_2");
    }

    @Test
    void testBucketizeForInstantValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(CAST('2024-06-15 10:00:30' as TIMESTAMP_LTZ), ARRAY[CAST('2024-06-15 10:00:00' as TIMESTAMP_LTZ), CAST('2024-06-15 10:00:20' as TIMESTAMP_LTZ)]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_3");
    }

    @Test
    void testBucketizeForLocalPeriodValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(INTERVAL '12 11' DAY(2) TO HOUR, ARRAY[INTERVAL '12 12' DAY(2) TO HOUR, INTERVAL '12 20' DAY(2) TO HOUR]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_1");
    }

    @Test
    void testBucketizeForNaNValue() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(CAST('NaN' AS DOUBLE), ARRAY[1, 1000]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_NULL");
    }

    @Test
    void testBucketizeForVeryLargeValues() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(CAST(1234567890123456799 AS BIGINT), ARRAY[CAST(1234567890123456791 AS BIGINT), CAST(1234567890123456799 AS BIGINT)]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_3");
    }

    @Test
    void testMLBucketizeForNaNInSplitBucketArray() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(1, ARRAY[CAST('NaN' as DOUBLE), 1]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_2");
    }

    @Test
    void testMLBucketizeForNullInSplitBucketArray() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_BUCKETIZE(0, ARRAY[NULL, 1]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_1");
    }

    @Test
    void testBucketizeForDuplicateValues() {
        final TableResult mlqueryResult1 =
                tableEnv.executeSql("SELECT ML_BUCKETIZE(2, ARRAY[1, 4, 4, 7]) AS scaled_value\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo("bin_2");
    }

    @Test
    void testMLBucketizeScalarForStringValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALAR('1', ARRAY[1, 5]) AS scaled_value\n;"));
    }

    @Test
    void testMLBucketizeScalarForStringArrayValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_MIN_MAX_SCALAR(1, ARRAY['1', '5']) AS scaled_value\n;"));
    }

    @Test
    void testMLBucketizeNonArraySecondArgument() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_BUCKETIZE(1, 1, ARRAY['b1', 'b2']) AS scaled_value\n;"));
    }

    @Test
    void testBucketizeForEmptySplitBucketArray() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_BUCKETIZE(4, ARRAY[]) AS scaled_value\n;"));
    }

    @Test
    void testMLBucketizeNonArrayThirdArgument() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_BUCKETIZE(1, ARRAY[1], 1) AS scaled_value\n;"));
    }

    @Test
    void testMLBucketizeForNonSortedSplitBucketArrayWithNames() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(1, ARRAY[1, 0], ARRAY['b_null', 'b1', 'b2', 'b3']) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLBucketizeForNULLSplitBucketArrayWithNames() {
        final TableResult result =
                tableEnv.executeSql(
                        "SELECT ML_BUCKETIZE(1, ARRAY[NULL, 1], ARRAY['b_null', 'b1', 'b2', 'b3']) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }

    @Test
    void testMLBucketizeForUnsortedSplitBucketArray() {
        final TableResult result =
                tableEnv.executeSql("SELECT ML_BUCKETIZE(1, ARRAY[2, 1]) AS scaled_value\n;");
        assertThrows(RuntimeException.class, () -> result.collect().next());
    }
}
