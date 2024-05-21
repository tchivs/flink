/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.otlp;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** OpenTelemetry Protocol Functions module. */
public class OtlpFunctionsModule implements Module {

    public static final OtlpFunctionsModule INSTANCE = new OtlpFunctionsModule();

    private final Map<String, FunctionDefinition> normalizedFunctions;

    public OtlpFunctionsModule() {
        normalizedFunctions =
                ImmutableMap.of(
                        OtlpMetricsDataTableFunction.NAME, new OtlpMetricsDataTableFunction(),
                        OtlpExampleMetrics.NAME, new OtlpExampleMetrics());
    }

    @Override
    public Set<String> listFunctions() {
        return normalizedFunctions.keySet();
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        final String normalizedName = name.toUpperCase(Locale.ROOT);
        return Optional.ofNullable(normalizedFunctions.get(normalizedName));
    }
}
