/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.functions.scalar.ai;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.functions.FunctionKind.SCALAR;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.InputTypeStrategies.varyingSequence;
import static org.apache.flink.table.types.inference.TypeStrategies.explicit;
import static org.apache.flink.table.types.inference.TypeStrategies.nullableIfArgs;

/** Module to provide OpenAI built-in functions. */
public class AIFunctionsModule implements Module {
    public static final BuiltInFunctionDefinition INVOKE_OPENAI =
            BuiltInFunctionDefinition.newBuilder()
                    .name("INVOKE_OPENAI")
                    .kind(SCALAR)
                    .inputTypeStrategy(
                            or(
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING)),
                                    sequence(
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING),
                                            logical(LogicalTypeFamily.CHARACTER_STRING))))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .runtimeClass(
                            "io.confluent.flink.table.functions.scalar.ai.AIResponseGenerator")
                    .build();
    public static final BuiltInFunctionDefinition SECRET =
            BuiltInFunctionDefinition.newBuilder()
                    .name("SECRET")
                    .kind(SCALAR)
                    .inputTypeStrategy(varyingSequence(logical(LogicalTypeFamily.CHARACTER_STRING)))
                    .outputTypeStrategy(nullableIfArgs(explicit(DataTypes.STRING())))
                    .runtimeClass("io.confluent.flink.table.functions.scalar.ai.AISecret")
                    .build();

    public static final AIFunctionsModule INSTANCE = new AIFunctionsModule();
    private final Map<String, BuiltInFunctionDefinition> normalizedFunctions;

    public AIFunctionsModule() {
        normalizedFunctions =
                Arrays.asList(INVOKE_OPENAI, SECRET).stream()
                        .collect(
                                Collectors.toMap(
                                        f -> f.getName().toUpperCase(Locale.ROOT),
                                        Function.identity()));
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
