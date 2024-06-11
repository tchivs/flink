/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Module to provide ML Prediction built-in functions. */
public class MLFunctionsModule implements Module {
    Map<String, FunctionDefinition> normalizedFunctions;

    public MLFunctionsModule() {
        this.normalizedFunctions =
                new HashMap<String, FunctionDefinition>() {
                    {
                        put(
                                MLPredictFunction.NAME,
                                new MLPredictFunction(
                                        MLPredictFunction.NAME, Collections.emptyMap()));
                        put(
                                MLEvaluateFunction.NAME,
                                new MLEvaluateFunction(
                                        MLEvaluateFunction.NAME, Collections.emptyMap()));
                        put(
                                MLEvaluateAllFunction.NAME,
                                new MLEvaluateAllFunction(
                                        MLEvaluateAllFunction.NAME,
                                        new ModelVersions("", Collections.emptyMap())));
                        put(
                                MLMinMaxScalarFunction.NAME,
                                new MLMinMaxScalarFunction(MLMinMaxScalarFunction.NAME));
                    }
                };
    }

    @Override
    public Set<String> listFunctions() {
        return normalizedFunctions.keySet();
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        final String normalizedName = name.toUpperCase(Locale.ROOT);
        return Optional.ofNullable(this.normalizedFunctions.get(normalizedName));
    }
}
