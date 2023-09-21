/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.core;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlOperator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CoreProxyModule}. */
@Confluent
public class CoreProxyModuleTest {

    @Test
    void testListing() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnv.unloadModule("core");
        tableEnv.loadModule("core", CoreProxyModule.INSTANCE);

        assertThat(tableEnv.listFunctions()).hasSameElementsAs(CoreProxyModule.PUBLIC_LIST);
    }

    @Test
    void testUnsupportedFunctions() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnv.unloadModule("core");
        tableEnv.loadModule("core", CoreProxyModule.INSTANCE);

        assertThatThrownBy(() -> tableEnv.executeSql("CREATE TABLE t (i INT, p AS PROCTIME())"))
                .hasMessageContaining(
                        "Function 'PROCTIME' is not supported in Confluent's Flink SQL dialect.");
    }

    @Test
    void testHiddenFunctions() {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tableEnv.unloadModule("core");
        tableEnv.loadModule("core", CoreProxyModule.INSTANCE);

        tableEnv.executeSql(
                "CREATE TABLE MyTable (i INT, t TIMESTAMP_LTZ(3), WATERMARK FOR t AS t) "
                        + "WITH ('connector' = 'datagen')");

        // Although, TUMBLE is not publicly listed (because it is deprecated)
        // it can still be resolved successfully.
        assertThat(
                        tableEnv.sqlQuery(
                                        "SELECT\n"
                                                + "  i,\n"
                                                + "  COUNT(i) FROM MyTable\n"
                                                + "GROUP BY\n"
                                                + "  TUMBLE(t, INTERVAL '1' DAY),\n"
                                                + "  i")
                                .getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("i", DataTypes.INT()),
                                Column.physical("EXPR$1", DataTypes.BIGINT().notNull())));
    }

    @Test
    void changeDetectionTest() {
        final Stream<String> allFunctions =
                Stream.concat(
                        Stream.concat(getCalciteFunctions(), getFlinkFunctions()),
                        getCalciteOperators());

        assertThat(normalizeList(allFunctions)).containsAll(FLINK_1_17);
    }

    /**
     * Code in case you want to update the function list when upgrading to a new Flink version.
     *
     * <p>Note: Keep in mind that this is still a manual process. Not every function makes sense to
     * expose to users. Also, some {@link SqlOperator}s can't be categorized as "functions" in the
     * traditional sense.
     */
    @Test
    @Disabled
    void printFunctions() {
        System.out.println("Function-style syntax:");
        printList(Stream.concat(getCalciteFunctions(), getFlinkFunctions()));

        System.out.println("SQL standard prefix/infix/postfix syntax:");
        printList(getCalciteOperators());
    }

    private static void printList(Stream<String> stream) {
        normalizeList(stream).map(n -> String.format("\"%s\",", n)).forEach(System.out::println);
    }

    private static Stream<String> normalizeList(Stream<String> stream) {
        return stream.filter(n -> !n.contains("$")).map(String::toUpperCase).distinct().sorted();
    }

    private static Stream<String> getCalciteFunctions() {
        return FlinkSqlOperatorTable.instance(false).getOperatorList().stream()
                .filter(f -> f instanceof SqlFunction)
                .map(SqlOperator::getName);
    }

    private static Stream<String> getCalciteOperators() {
        return FlinkSqlOperatorTable.instance(false).getOperatorList().stream()
                .filter(f -> !(f instanceof SqlFunction) && !(f instanceof SqlInternalOperator))
                .map(SqlOperator::getName);
    }

    private static Stream<String> getFlinkFunctions() {
        return BuiltInFunctionDefinitions.getDefinitions().stream()
                .map(BuiltInFunctionDefinition.class::cast)
                .filter(BuiltInFunctionDefinition::hasRuntimeImplementation)
                .map(BuiltInFunctionDefinition::getName);
    }

    // --------------------------------------------------------------------------------------------
    // Function names
    // --------------------------------------------------------------------------------------------

    /**
     * Snapshot of functions for {@link #changeDetectionTest()}.
     *
     * <p>Use {@link #printFunctions()} to regenerate if necessary.
     */
    private static final List<String> FLINK_1_17 =
            Arrays.asList(
                    // Function-style syntax:
                    "ABS",
                    "ACOS",
                    "AGG_DECIMAL_MINUS",
                    "AGG_DECIMAL_PLUS",
                    "APPROX_COUNT_DISTINCT",
                    "ARRAY_CONTAINS",
                    "ASCII",
                    "ASIN",
                    "ATAN",
                    "ATAN2",
                    "AUXILIARY_GROUP",
                    "AVG",
                    "BIN",
                    "CARDINALITY",
                    "CAST",
                    "CEIL",
                    "CHARACTER_LENGTH",
                    "CHAR_LENGTH",
                    "CHR",
                    "CLASSIFIER",
                    "COALESCE",
                    "COLLECT",
                    "CONCAT",
                    "CONCAT_WS",
                    "CONVERT_TZ",
                    "COS",
                    "COSH",
                    "COT",
                    "COUNT",
                    "CUME_DIST",
                    "CUMULATE",
                    "CURRENT_DATABASE",
                    "CURRENT_DATE",
                    "CURRENT_ROW_TIMESTAMP",
                    "CURRENT_TIME",
                    "CURRENT_TIMESTAMP",
                    "CURRENT_WATERMARK",
                    "DATE_FORMAT",
                    "DAYOFMONTH",
                    "DAYOFWEEK",
                    "DAYOFYEAR",
                    "DECODE",
                    "DEGREES",
                    "DENSE_RANK",
                    "E",
                    "ELEMENT",
                    "ENCODE",
                    "EXP",
                    "EXTRACT",
                    "FIRST",
                    "FIRST_VALUE",
                    "FLOOR",
                    "FROM_BASE64",
                    "FROM_UNIXTIME",
                    "GREATEST",
                    "GROUPING",
                    "GROUPING_ID",
                    "GROUP_ID",
                    "HASH_CODE",
                    "HEX",
                    "HIVE_AGG_DECIMAL_PLUS",
                    "HOP",
                    "HOP_END",
                    "HOP_PROCTIME",
                    "HOP_ROWTIME",
                    "HOP_START",
                    "HOUR",
                    "IF",
                    "IFNULL",
                    "INITCAP",
                    "INSTR",
                    "IS_ALPHA",
                    "IS_DECIMAL",
                    "IS_DIGIT",
                    "JSON_ARRAY",
                    "JSON_ARRAYAGG_ABSENT_ON_NULL",
                    "JSON_ARRAYAGG_NULL_ON_NULL",
                    "JSON_EXISTS",
                    "JSON_OBJECT",
                    "JSON_OBJECTAGG_ABSENT_ON_NULL",
                    "JSON_OBJECTAGG_NULL_ON_NULL",
                    "JSON_QUERY",
                    "JSON_STRING",
                    "JSON_VALUE",
                    "LAG",
                    "LAST",
                    "LAST_VALUE",
                    "LEAD",
                    "LEAST",
                    "LEFT",
                    "LISTAGG",
                    "LN",
                    "LOCALTIME",
                    "LOCALTIMESTAMP",
                    "LOCATE",
                    "LOG",
                    "LOG10",
                    "LOG2",
                    "LOWER",
                    "LPAD",
                    "LTRIM",
                    "MATCH_PROCTIME",
                    "MATCH_ROWTIME",
                    "MAX",
                    "MD5",
                    "MIN",
                    "MINUTE",
                    "MOD",
                    "MONTH",
                    "NEXT",
                    "NOW",
                    "NTILE",
                    "NULLIF",
                    "OVERLAY",
                    "PARSE_URL",
                    "PERCENT_RANK",
                    "PI",
                    "POSITION",
                    "POWER",
                    "PREV",
                    "PRINT",
                    "PROCTIME",
                    "PROCTIME_MATERIALIZE",
                    "QUARTER",
                    "RADIANS",
                    "RAND",
                    "RAND_INTEGER",
                    "RANK",
                    "REGEXP",
                    "REGEXP_EXTRACT",
                    "REGEXP_REPLACE",
                    "REPEAT",
                    "REPLACE",
                    "REVERSE",
                    "RIGHT",
                    "ROUND",
                    "ROW_NUMBER",
                    "RPAD",
                    "RTRIM",
                    "SECOND",
                    "SESSION_END",
                    "SESSION_PROCTIME",
                    "SESSION_ROWTIME",
                    "SESSION_START",
                    "SHA1",
                    "SHA2",
                    "SHA224",
                    "SHA256",
                    "SHA384",
                    "SHA512",
                    "SIGN",
                    "SIN",
                    "SINGLE_VALUE",
                    "SINH",
                    "SOURCE_WATERMARK",
                    "SPLIT_INDEX",
                    "SQRT",
                    "STDDEV",
                    "STDDEV_POP",
                    "STDDEV_SAMP",
                    "STREAMRECORD_TIMESTAMP",
                    "STR_TO_MAP",
                    "SUBSTR",
                    "SUBSTRING",
                    "SUM",
                    "TAN",
                    "TANH",
                    "TIMESTAMPADD",
                    "TIMESTAMPDIFF",
                    "TO_BASE64",
                    "TO_DATE",
                    "TO_TIMESTAMP",
                    "TO_TIMESTAMP_LTZ",
                    "TRIM",
                    "TRUNCATE",
                    "TRY_CAST",
                    "TUMBLE",
                    "TUMBLE_END",
                    "TUMBLE_PROCTIME",
                    "TUMBLE_ROWTIME",
                    "TUMBLE_START",
                    "TYPEOF",
                    "UNIX_TIMESTAMP",
                    "UPPER",
                    "UUID",
                    "VARIANCE",
                    "VAR_POP",
                    "VAR_SAMP",
                    "WEEK",
                    "YEAR",
                    // SQL standard prefix/infix/postfix syntax:
                    "%",
                    "*",
                    "+",
                    "-",
                    "/",
                    "/INT",
                    "<",
                    "<=",
                    "<>",
                    "=",
                    ">",
                    ">=",
                    "AND",
                    "ARRAY",
                    "AS",
                    "BETWEEN ASYMMETRIC",
                    "BETWEEN SYMMETRIC",
                    "CASE",
                    "DESC",
                    "DESCRIPTOR",
                    "DOT",
                    "EXCEPT",
                    "EXCEPT ALL",
                    "EXISTS",
                    "FINAL",
                    "IN",
                    "INTERSECT",
                    "INTERSECT ALL",
                    "IS DISTINCT FROM",
                    "IS FALSE",
                    "IS JSON ARRAY",
                    "IS JSON OBJECT",
                    "IS JSON SCALAR",
                    "IS JSON VALUE",
                    "IS NOT DISTINCT FROM",
                    "IS NOT FALSE",
                    "IS NOT JSON ARRAY",
                    "IS NOT JSON OBJECT",
                    "IS NOT JSON SCALAR",
                    "IS NOT JSON VALUE",
                    "IS NOT NULL",
                    "IS NOT TRUE",
                    "IS NOT UNKNOWN",
                    "IS NULL",
                    "IS TRUE",
                    "IS UNKNOWN",
                    "ITEM",
                    "LIKE",
                    "MAP",
                    "MULTISET",
                    "NOT",
                    "NOT BETWEEN ASYMMETRIC",
                    "NOT BETWEEN SYMMETRIC",
                    "NOT IN",
                    "NOT LIKE",
                    "NOT SIMILAR TO",
                    "NULLS FIRST",
                    "NULLS LAST",
                    "OR",
                    "OVERLAPS",
                    "REINTERPRET",
                    "ROW",
                    "RUNNING",
                    "SIMILAR TO",
                    "UNION",
                    "UNION ALL",
                    "||");
}
