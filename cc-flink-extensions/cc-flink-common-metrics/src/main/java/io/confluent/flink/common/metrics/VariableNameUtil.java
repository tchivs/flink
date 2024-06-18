/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

/**
 * Util for dealing with variables names in OTel reporting. Used to remove angel brackets that
 * metric group codes adds to variables.
 */
public class VariableNameUtil {
    private VariableNameUtil() {}

    /** Removes leading and trailing angle brackets. See ScopeFormat::SCOPE_VARIABLE_PREFIX. */
    public static String getVariableName(String str) {
        if (str.startsWith("<") && str.endsWith(">")) {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }
}
