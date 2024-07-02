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

/** Unit tests for the {@link MLNGramsFunction} class. */
class MLNGramsFunctionTest {

    private TableEnvironment tableEnv;

    @BeforeEach
    public void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        tableEnv = TableEnvironment.create(settings);
        // Registering temp function
        final String functionName = "ML_NGRAMS";
        tableEnv.createTemporaryFunction(functionName, new MLNGramsFunction(functionName));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
    }

    @Test
    void testNGramsFunction() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab','cd','de','pwe'], 1, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab", "cd", "de", "pwe"});
    }

    @Test
    void testNGramsFunctionWithNullValueInArray() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab','cd','de', NULL], 2, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab#cd", "cd#de"});
    }

    @Test
    void testNGramsFunctionWithNullValueInArrayInMiddle() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab', NULL,'cd','de'], 2, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab#cd", "cd#de"});
    }

    @Test
    void testNGramsFunctionWithEmptyStringInArray() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab','cd','de', ''], 2, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab#cd", "cd#de", "de#"});
    }

    @Test
    void testNGramsFunctionWithWhitespaceAtEndInArray() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab','cd','de  ', 'fg'], 2, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab#cd", "cd#de  ", "de  #fg"});
    }

    @Test
    void testNGramsFunctionWithMValueGtArrayLength() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab','cd','de  ', 'fg'], 5, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {});
    }

    @Test
    void testNGramsFunctionWithNullInputArray() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_NGRAMS(NULL, 0) as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {});
    }

    @Test
    void testNGramsFunctionWithMValueEtArrayLength() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab','cd','de  ', 'fg'], 4, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab#cd#de  #fg"});
    }

    @Test
    void testNGramsFunctionWithoutOptionalPrams() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab','cd','de','pwe']) as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab cd", "cd de", "de pwe"});
    }

    @Test
    void testNGramsFunctionWithoutSeparator() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab','cd','de','pwe'], 3) as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab cd de", "cd de pwe"});
    }

    @Test
    void testNGramsFunctionWithSingleElementInArray() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_NGRAMS(ARRAY['ab'], 2, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {});
    }

    @Test
    void testNGramsFunctionWithSingleElementArrayAndNValueOne() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_NGRAMS(ARRAY['ab'], 1, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab"});
    }

    @Test
    void testNGramsFunctionWithMixedWhitespaceAndEmptyStrings() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab', ' ', '', 'cd'], 2, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab#", "#", "#cd"});
    }

    @Test
    void testNGramsFunctionWithSpecialCharacters() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab', 'cd!', '@de', 'f$'], 2, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab#cd!", "cd!#@de", "@de#f$"});
    }

    @Test
    void testNGramsFunctionWithDuplicateStrings() {
        final TableResult tableResult =
                tableEnv.executeSql(
                        "SELECT ML_NGRAMS(ARRAY['ab', 'ab', 'cd', 'cd'], 2, '#') as scaled_value;");
        Row row = tableResult.collect().next();
        assertThat(row.getField(0)).isEqualTo(new String[] {"ab#ab", "ab#cd", "cd#cd"});
    }

    @Test
    void testNGramsFunctionWithLtOneNValue() {
        final TableResult tableResult =
                tableEnv.executeSql("SELECT ML_NGRAMS(ARRAY['ab','cd'], 0) as scaled_value;");
        assertThrows(RuntimeException.class, () -> tableResult.collect().next());
    }

    @Test
    void testNGramsFunctionWithEmptyInputArray() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_NGRAMS(ARRAY[], 1, '#') as scaled_value;"));
    }

    @Test
    void testNGramsFunctionWithNullNValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_NGRAMS(ARRAY['ab','cd'], NULL, '#' ) as scaled_value;"));
    }

    @Test
    void testNGramsFunctionWithNullSeparator() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_NGRAMS(ARRAY['ab','cd'], 2, NULL ) as scaled_value;"));
    }

    @Test
    void testNGramsFunctionWithInvalidDataTypeInput() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_NGRAMS('N', 0) as scaled_value;"));
    }

    @Test
    void testNGramsFunctionWithInvalidDataTypeNValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_NGRAMS(ARRAY['a','b'], 'c') as scaled_value;"));
    }

    @Test
    void testNGramsFunctionWithInvalidDataTypeSeparatorValue() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_NGRAMS(ARRAY['a','b'], 2, 3) as scaled_value;"));
    }

    @Test
    void testNGramsFunctionWithAllNullParameters() {
        assertThrows(
                ValidationException.class,
                () -> tableEnv.executeSql("SELECT ML_NGRAMS(NULL, NULL, NULL) as scaled_value;"));
    }

    @Test
    void testNGramsFunctionWithMostNullValuesInArray() {
        assertThrows(
                ValidationException.class,
                () ->
                        tableEnv.executeSql(
                                "SELECT ML_NGRAMS(ARRAY[NULL, NULL, NULL], 2, '#') as scaled_value;"));
    }
}
