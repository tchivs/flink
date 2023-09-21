/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.core;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.Module;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A proxy layer in front of {@link CoreModule} for a curated list of supported built-in functions.
 *
 * <p>Due to historical reasons, the Flink function stack is currently split into a Calcite and a
 * Flink one. Also, the Flink one had multiple iterations. This proxy module aims to hide the mess
 * and also forbid certain functions that we clearly don't want to support (e.g. PRINT).
 */
@Confluent
public class CoreProxyModule implements Module {

    public static final CoreProxyModule INSTANCE = new CoreProxyModule();

    /**
     * Set of functions (declared either in Calcite's or Flink's new function stack) that can be
     * called using function-style syntax (i.e. "f()").
     */
    public static final Set<String> PUBLIC_FUNCTIONS = initPublicFunctions();

    /**
     * Set of functions (declared in Calcite's stack) that can be called using prefix/infix/postfix
     * syntax (e.g. "BETWEEN ASYMMETRIC").
     */
    public static final Set<String> PUBLIC_OPERATORS = initPublicOperators();

    /** Full curated list that we want to expose to users. */
    public static final Set<String> PUBLIC_LIST =
            Stream.concat(PUBLIC_FUNCTIONS.stream(), PUBLIC_OPERATORS.stream())
                    .collect(Collectors.toSet());

    /** List of unsupported functions/operators. */
    public static final Set<String> UNSUPPORTED = initUnsupported();

    @Override
    public Set<String> listFunctions() {
        return CoreModule.INSTANCE.listFunctions();
    }

    @Override
    public Set<String> listFunctions(boolean includeHiddenFunctions) {
        if (includeHiddenFunctions) {
            return CoreModule.INSTANCE.listFunctions(true);
        }
        return PUBLIC_LIST;
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        final String normalizedName = name.toUpperCase(Locale.ROOT);
        if (UNSUPPORTED.contains(normalizedName)) {
            throw new ValidationException(
                    String.format(
                            "Function '%s' is not supported in Confluent's Flink SQL dialect.",
                            normalizedName));
        }
        return CoreModule.INSTANCE.getFunctionDefinition(normalizedName);
    }

    // --------------------------------------------------------------------------------------------
    // Function names
    // --------------------------------------------------------------------------------------------

    private static Set<String> initPublicFunctions() {
        final List<String> names =
                Arrays.asList(
                        "ABS",
                        "ACOS",
                        "ARRAY_CONTAINS",
                        "ASCII",
                        "ASIN",
                        "ATAN",
                        "ATAN2",
                        "AVG",
                        "BIN",
                        "CARDINALITY",
                        "CAST",
                        "CEIL",
                        "CHARACTER_LENGTH",
                        "CHAR_LENGTH",
                        "CHR",
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
                        "FIRST_VALUE",
                        "FLOOR",
                        "FROM_BASE64",
                        "FROM_UNIXTIME",
                        "GREATEST",
                        "GROUPING",
                        "GROUPING_ID",
                        "GROUP_ID",
                        "HEX",
                        "HOUR",
                        "IF",
                        "IFNULL",
                        "INITCAP",
                        "INSTR",
                        "IS_ALPHA",
                        "IS_DECIMAL",
                        "IS_DIGIT",
                        "JSON_ARRAY",
                        "JSON_EXISTS",
                        "JSON_OBJECT",
                        "JSON_QUERY",
                        "JSON_STRING",
                        "JSON_VALUE",
                        "LAG",
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
                        "SHA1",
                        "SHA2",
                        "SHA224",
                        "SHA256",
                        "SHA384",
                        "SHA512",
                        "SIGN",
                        "SIN",
                        "SINH",
                        "SOURCE_WATERMARK",
                        "SPLIT_INDEX",
                        "SQRT",
                        "STDDEV",
                        "STDDEV_POP",
                        "STDDEV_SAMP",
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
                        "TYPEOF",
                        "UNIX_TIMESTAMP",
                        "UPPER",
                        "UUID",
                        "VARIANCE",
                        "VAR_POP",
                        "VAR_SAMP",
                        "WEEK",
                        "YEAR");
        return Collections.unmodifiableSet(new HashSet<>(names));
    }

    private static Set<String> initPublicOperators() {
        final List<String> names =
                Arrays.asList(
                        "%",
                        "*",
                        "+",
                        "-",
                        "/",
                        "<",
                        "<=",
                        "<>",
                        "=",
                        ">",
                        ">=",
                        "AND",
                        "ARRAY",
                        "BETWEEN ASYMMETRIC",
                        "BETWEEN SYMMETRIC",
                        "CASE",
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
                        "LIKE",
                        "MAP",
                        "MULTISET",
                        "NOT",
                        "NOT BETWEEN ASYMMETRIC",
                        "NOT BETWEEN SYMMETRIC",
                        "NOT IN",
                        "NOT LIKE",
                        "NOT SIMILAR TO",
                        "OR",
                        "OVERLAPS",
                        "ROW",
                        "SIMILAR TO",
                        "||");
        return Collections.unmodifiableSet(new HashSet<>(names));
    }

    private static Set<String> initUnsupported() {
        final List<String> names =
                Arrays.asList(
                        "MATCH_PROCTIME",
                        "PRINT",
                        "PROCTIME",
                        "PROCTIME_MATERIALIZE",
                        "STREAMRECORD_TIMESTAMP");
        return Collections.unmodifiableSet(new HashSet<>(names));
    }
}
