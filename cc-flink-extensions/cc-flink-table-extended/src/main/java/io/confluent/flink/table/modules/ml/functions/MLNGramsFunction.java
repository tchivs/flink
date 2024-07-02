/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static io.confluent.flink.table.utils.mlutils.MlFunctionsUtil.getTypeInferenceForMLScalarFunctions;

/** A scalar function that generates n-grams from an array of strings. */
public class MLNGramsFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_NGRAMS";
    /** The name of the function. */
    public final String functionName;

    /**
     * Constructs a new MLNGramsFunction.
     *
     * @param functionName The name of the function.
     */
    public MLNGramsFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Generates n-grams from an array of strings with default n-value and separator.
     *
     * @param input The input array of strings.
     * @return An array of n-grams.
     */
    public String[] eval(Object input) {
        return eval(input, 2, " ");
    }

    /**
     * Generates n-grams from an array of strings with specified n-value and default separator.
     *
     * @param input The input array of strings.
     * @param nValue The number of elements in each n-gram.
     * @return An array of n-grams.
     */
    public String[] eval(Object input, Integer nValue) {
        return eval(input, nValue, " ");
    }

    /**
     * Generates n-grams from an array of strings with specified n-value and separator.
     *
     * @param input The input array of strings.
     * @param nValue The number of elements in each n-gram.
     * @param separator The separator to use between elements in each n-gram.
     * @return An array of n-grams.
     */
    public String[] eval(Object input, Integer nValue, String separator) {
        try {
            if (Objects.isNull(input)
                    || ((String[]) input).length == 0
                    || nValue > ((String[]) input).length) {
                return new String[0];
            }
            if (nValue < 1) {
                throw new FlinkRuntimeException(
                        "N value argument to %s function must be greater than 0");
            }
            String[] inputArray = (String[]) input;
            String[] processedInput = preProcessInput(inputArray);
            String[] nGrams = new String[processedInput.length - nValue + 1];
            List<String> wordsList = new ArrayList<>();
            IntStream.range(0, processedInput.length)
                    .forEach(
                            i -> {
                                String currentWord = processedInput[i];
                                currentWord = currentWord.trim().isEmpty() ? "" : currentWord;
                                wordsList.add(currentWord);
                                if (wordsList.size() >= nValue) {
                                    if (wordsList.size() > nValue) {
                                        wordsList.remove(0);
                                    }
                                    nGrams[i - nValue + 1] = String.join(separator, wordsList);
                                }
                            });
            return nGrams;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    /**
     * Provides type inference for the MLNGramsFunction.
     *
     * @param typeFactory The factory to create data types.
     * @return The type inference for the function.
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return getTypeInferenceForMLScalarFunctions(
                1,
                3,
                DataTypes.ARRAY(DataTypes.STRING()),
                count -> count > 0 && count < 4,
                this::validateInputTypes);
    }

    private Optional<String> validateInputTypes(List<DataType> argumentDataTypes) {
        return IntStream.range(0, argumentDataTypes.size())
                .mapToObj(
                        i -> {
                            if (i > 0 && argumentDataTypes.get(i).equals(DataTypes.NULL())) {
                                return String.format(
                                        "Argument %s to %s function cannot be NULL.", i, NAME);
                            }
                            switch (i) {
                                case 0:
                                    if (!argumentDataTypes.get(i).equals(DataTypes.NULL())
                                            && !argumentDataTypes
                                                    .get(i)
                                                    .getConversionClass()
                                                    .isArray()
                                            && argumentDataTypes
                                                    .get(i)
                                                    .getChildren()
                                                    .get(0)
                                                    .getLogicalType()
                                                    .isAnyOf(LogicalTypeFamily.CHARACTER_STRING)) {
                                        return String.format(
                                                "Argument %s to %s function needs to be an ARRAY of type STRING.",
                                                i + 1, NAME);
                                    }
                                    break;
                                case 1:
                                    if (!argumentDataTypes
                                            .get(i)
                                            .nullable()
                                            .equals(DataTypes.INT())) {
                                        return String.format(
                                                "Argument %s to %s function needs to be an INTEGER value.",
                                                i + 1, NAME);
                                    }
                                    break;
                                case 2:
                                    if (!argumentDataTypes
                                            .get(i)
                                            .getLogicalType()
                                            .isAnyOf(LogicalTypeFamily.CHARACTER_STRING)) {
                                        return String.format(
                                                "Argument %s to %s function needs to be a STRING value.",
                                                i + 1, NAME);
                                    }
                                    break;
                            }
                            return null;
                        })
                .filter(Objects::nonNull)
                .findFirst();
    }

    private String[] preProcessInput(String[] input) {
        return Arrays.stream(input).filter(Objects::nonNull).toArray(String[]::new);
    }
}
