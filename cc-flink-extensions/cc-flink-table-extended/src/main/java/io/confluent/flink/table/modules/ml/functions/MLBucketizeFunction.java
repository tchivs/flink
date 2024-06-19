/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlFunctionsUtil;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

/** A scaler function that assigns a numerical expression to a bucket. */
public class MLBucketizeFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_BUCKETIZE";
    /** The name of the function. */
    public final String functionName;

    private final String bucketNameInitial = "bin_";

    /**
     * Constructor for the MLBucketizeFunction class.
     *
     * @param functionName the name of the function.
     */
    public MLBucketizeFunction(String functionName) {
        this.functionName = functionName;
    }

    /**
     * Evaluates the input arguments and returns the corresponding bucket name.
     *
     * @param args the input arguments where: - args[0] is the input value, - args[1] is an array
     *     representing the bucket splits, - args[2] (optional) is an array of bucket names.
     * @return the bucket name as a string.
     */
    public String eval(Object... args) {
        try {
            Object value = args[0];
            Object arraySplitPoints = args[1];
            Optional<String[]> bucketNames = Optional.empty();
            if (args.length == 3) {
                bucketNames = Optional.of((castToArray(args[2], String.class)));
            }
            if (value instanceof Long && arraySplitPoints instanceof Long[]) {
                Long[] splitBucketsLong = castToArray(arraySplitPoints, Long.class);
                bucketNames.ifPresent(names -> validateBucketNames(splitBucketsLong, names));
                return mapLongValueToBucket(
                        MlFunctionsUtil.getLongValue(value, NAME),
                        splitBucketsLong,
                        bucketNames.orElse(new String[0]));
            } else {
                Double[] splitBucketsDouble = castToArray(arraySplitPoints, Double.class);
                bucketNames.ifPresent(names -> validateBucketNames(splitBucketsDouble, names));
                return mapDoubleValuesToBucket(
                        MlFunctionsUtil.getDoubleValue(value, NAME),
                        splitBucketsDouble,
                        bucketNames.orElse(new String[0]));
            }
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    private String mapDoubleValuesToBucket(
            Double value, Double[] arraySplitPoints, String[] bucketNames) {
        boolean isNamePresent = bucketNames.length > 0;
        if (Objects.isNull(value) || value.isNaN()) {
            return !isNamePresent ? bucketNameInitial + "NULL" : bucketNames[0];
        }
        if (value.isInfinite()) {
            return value < 0
                    ? !isNamePresent ? bucketNameInitial + 1 : bucketNames[1]
                    : !isNamePresent
                            ? bucketNameInitial + (arraySplitPoints.length + 1)
                            : bucketNames[bucketNames.length - 1];
        }
        return mapFiniteValueToBucket(value, arraySplitPoints, bucketNames);
    }

    private String mapLongValueToBucket(Long value, Long[] arraySplitPoints, String[] bucketNames) {
        boolean isNamePresent = bucketNames.length > 0;
        if (Objects.isNull(value)) {
            return !isNamePresent ? bucketNameInitial + "NULL" : bucketNames[0];
        }
        return mapFiniteValueToBucket(value, arraySplitPoints, bucketNames);
    }

    private <T extends Comparable<T>> String mapFiniteValueToBucket(
            T value, T[] arraySplitPoints, String[] bucketNames) {
        validateSplitArrayOrder(arraySplitPoints);
        boolean isNamePresent = bucketNames.length > 0;
        for (int i = 0; i < arraySplitPoints.length; i++) {
            if (value.compareTo(arraySplitPoints[i]) < 0) {
                return !isNamePresent ? bucketNameInitial + (i + 1) : bucketNames[i + 1];
            }
        }
        return !isNamePresent
                ? bucketNameInitial + (arraySplitPoints.length + 1)
                : bucketNames[bucketNames.length - 1];
    }

    /**
     * Provides type inference for the function.
     *
     * @param typeFactory the DataTypeFactory.
     * @return the TypeInference object.
     */
    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(
                        new InputTypeStrategy() {
                            @Override
                            public ArgumentCount getArgumentCount() {
                                return new ArgumentCount() {
                                    @Override
                                    public boolean isValidCount(int count) {
                                        return count == 2 || count == 3;
                                    }

                                    @Override
                                    public Optional<Integer> getMinCount() {
                                        return Optional.of(2);
                                    }

                                    @Override
                                    public Optional<Integer> getMaxCount() {
                                        return Optional.of(3);
                                    }
                                };
                            }

                            @Override
                            public Optional<List<DataType>> inferInputTypes(
                                    CallContext callContext, boolean throwOnFailure) {
                                List<DataType> argumentDataTypes =
                                        callContext.getArgumentDataTypes();

                                Optional<String> errorMessage =
                                        IntStream.range(0, argumentDataTypes.size())
                                                .mapToObj(
                                                        i -> {
                                                            DataType argDataType =
                                                                    argumentDataTypes.get(i);
                                                            switch (i) {
                                                                case (0):
                                                                    if (!argDataType.equals(
                                                                                    DataTypes
                                                                                            .NULL())
                                                                            && !argDataType
                                                                                    .getLogicalType()
                                                                                    .isAnyOf(
                                                                                            LogicalTypeFamily
                                                                                                    .NUMERIC,
                                                                                            LogicalTypeFamily
                                                                                                    .TIME,
                                                                                            LogicalTypeFamily
                                                                                                    .TIMESTAMP,
                                                                                            LogicalTypeFamily
                                                                                                    .DATETIME,
                                                                                            LogicalTypeFamily
                                                                                                    .INTERVAL)) {
                                                                        return String.format(
                                                                                "%s data type is not supported as the first argument to %s function.",
                                                                                argDataType, NAME);
                                                                    }
                                                                    break;
                                                                case (1):
                                                                    if (argDataType
                                                                            .getConversionClass()
                                                                            .isArray()) {
                                                                        DataType childDataType =
                                                                                argDataType
                                                                                        .getChildren()
                                                                                        .get(0);
                                                                        if (!argDataType.equals(
                                                                                        DataTypes
                                                                                                .NULL())
                                                                                && !childDataType
                                                                                        .getLogicalType()
                                                                                        .isAnyOf(
                                                                                                LogicalTypeFamily
                                                                                                        .NUMERIC,
                                                                                                LogicalTypeFamily
                                                                                                        .TIME,
                                                                                                LogicalTypeFamily
                                                                                                        .TIMESTAMP,
                                                                                                LogicalTypeFamily
                                                                                                        .DATETIME,
                                                                                                LogicalTypeFamily
                                                                                                        .INTERVAL)) {
                                                                            return String.format(
                                                                                    "Array of type %s is not supported as the second argument to %s function.",
                                                                                    childDataType,
                                                                                    NAME);
                                                                        }
                                                                    } else {
                                                                        return String.format(
                                                                                "The second argument to the %s function must be as ARRAY.",
                                                                                NAME);
                                                                    }
                                                                    break;
                                                                case (2):
                                                                    if (argDataType
                                                                            .getConversionClass()
                                                                            .isArray()) {
                                                                        DataType childDataType =
                                                                                argDataType
                                                                                        .getChildren()
                                                                                        .get(0);
                                                                        if (!childDataType
                                                                                .getLogicalType()
                                                                                .isAnyOf(
                                                                                        LogicalTypeFamily
                                                                                                .NUMERIC,
                                                                                        LogicalTypeFamily
                                                                                                .TIME,
                                                                                        LogicalTypeFamily
                                                                                                .TIMESTAMP,
                                                                                        LogicalTypeFamily
                                                                                                .DATETIME,
                                                                                        LogicalTypeFamily
                                                                                                .INTERVAL,
                                                                                        LogicalTypeFamily
                                                                                                .CHARACTER_STRING)) {
                                                                            return String.format(
                                                                                    "Array of type %s is not supported as the third argument to %s function. ",
                                                                                    childDataType,
                                                                                    NAME);
                                                                        }
                                                                    } else {
                                                                        return String.format(
                                                                                "Third argument to %s function must be as ARRAY.",
                                                                                NAME);
                                                                    }
                                                                    break;
                                                                default:
                                                                    return String.format(
                                                                            "Invalid number of arguments passed to the %s function.",
                                                                            NAME);
                                                            }

                                                            return null;
                                                        })
                                                .filter(Objects::nonNull)
                                                .findFirst();

                                if (errorMessage.isPresent()) {
                                    if (throwOnFailure) {
                                        throw new ValidationException(errorMessage.get());
                                    } else {
                                        return Optional.empty();
                                    }
                                }
                                return Optional.of(argumentDataTypes);
                            }

                            @Override
                            public List<Signature> getExpectedSignatures(
                                    FunctionDefinition definition) {
                                final List<Signature.Argument> arguments = new ArrayList<>();
                                for (int i = 0; i < 3; i++) {
                                    arguments.add(Signature.Argument.of("input" + i));
                                }
                                return Collections.singletonList(Signature.of(arguments));
                            }
                        })
                .outputTypeStrategy(callContext -> Optional.of(DataTypes.STRING()))
                .build();
    }

    @Override
    public String toString() {
        return String.format("MLBucketizeFunction {functionName=%s}", functionName);
    }

    @SuppressWarnings("unchecked")
    private <T> T[] castToArray(Object arrayObject, Class<T> targetType) {
        int length = Array.getLength(arrayObject);
        List<T> typedList = new ArrayList<>();

        for (int i = 0; i < length; i++) {
            Object element = Array.get(arrayObject, i);
            T typedElement = castElement(element, targetType);

            if (shouldAddElement(typedElement, typedList, targetType)) {
                typedList.add(typedElement);
            }
        }

        return typedList.toArray((T[]) Array.newInstance(targetType, 0));
    }

    @SuppressWarnings("unchecked")
    private <T> T castElement(Object element, Class<T> targetType) {
        if (targetType.equals(Double.class)) {
            return (T) MlFunctionsUtil.getDoubleValue(element, NAME);
        } else if (targetType.equals(Long.class)) {
            return (T) MlFunctionsUtil.getLongValue(element, NAME);
        } else if (targetType.equals(String.class)) {
            return (T) (element == null ? null : String.valueOf(element));
        } else {
            throw new FlinkRuntimeException(
                    String.format(
                            "%s function does not support array of type %s as arguments.",
                            NAME, targetType.getSimpleName()));
        }
    }

    @SuppressWarnings("unchecked")
    private <T> boolean shouldAddElement(T element, List<T> typedList, Class<T> targetType) {
        if (Objects.nonNull(element) && targetType.equals(Double.class)) {
            Double doubleValue = (Double) element;
            return !doubleValue.isNaN() && isLastElementUnEqual((T) doubleValue, typedList);
        } else if (Objects.nonNull(element) && targetType.equals(Long.class)) {
            return isLastElementUnEqual(element, typedList);
        } else {
            return targetType.equals(String.class);
        }
    }

    private <T> boolean isLastElementUnEqual(T element, List<T> typedList) {
        return typedList.isEmpty() || !Objects.equals(element, typedList.get(typedList.size() - 1));
    }

    private <T extends Comparable<T>> void validateBucketNames(
            T[] arraySplitPoints, String[] bucketNames) {
        if (bucketNames.length != arraySplitPoints.length + 2) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Expecting %s bucket names, %s names were passed to %s function. "
                                    + "The first bucket name is for undefined values, followed by a name for each unique split",
                            arraySplitPoints.length + 2, bucketNames.length, NAME));
        }
    }

    private <T extends Comparable<T>> void validateSplitArrayOrder(T[] arraySplitPoints) {
        if (!isArraySorted(arraySplitPoints)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Invalid order of buckets split passed to the function %s. "
                                    + "arraySplitPoints must be sorted in ascending order.",
                            NAME));
        }
    }

    private <T extends Comparable<T>> boolean isArraySorted(T[] arraySplitPoints) {
        for (int i = 0; i < arraySplitPoints.length - 1; ++i) {
            if (arraySplitPoints[i].compareTo(arraySplitPoints[i + 1]) > 0) {
                return false;
            }
        }
        return true;
    }
}
