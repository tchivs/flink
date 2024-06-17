package io.confluent.flink.table.modules.ml.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlFunctionsUtil;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * A scaler function that assigns a numerical expression to a bucket.
 */
public class MLBucketizeFunction extends ScalarFunction {
    /** The name of the function. */
    public static final String NAME = "ML_BUCKETIZE";
    /** The name of the function. */
    public final String functionName;

    private final String bucketNameInitial = "bin_";

    private static final Set<DataType> DATA_TYPE_SET = MlFunctionsUtil.getBaseDataTypes();

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
     * @param args the input arguments.
     * @return the bucket name as a string.
     */
    public String eval(Object... args) {
        Object value = args[0];
        Object splitBuckets = args[1];
        Optional<String[]> bucketNames = Optional.empty();
        if (args.length == 3) {
            bucketNames = Optional.of((String[]) args[2]);
        }
        if (value instanceof Long && splitBuckets instanceof Long[]) {
            Long[] splitBucketsLong = castToNumberArray(splitBuckets, Long.class);
            bucketNames.ifPresent(names -> validateBucketNames(splitBucketsLong, names));
            return mapLongValueToBucket(
                    MlFunctionsUtil.getLongValue(value, NAME),
                    splitBucketsLong,
                    bucketNames.orElse(new String[0]));
        } else {
            Double[] splitBucketsDouble = castToNumberArray(splitBuckets, Double.class);
            bucketNames.ifPresent(names -> validateBucketNames(splitBucketsDouble, names));
            return mapDoubleValuesToBucket(
                    MlFunctionsUtil.getDoubleValue(value, NAME),
                    splitBucketsDouble,
                    bucketNames.orElse(new String[0]));
        }
    }

    private String mapDoubleValuesToBucket(
            Double value, Double[] splitBuckets, String[] bucketNames) {
        boolean isNamePresent = bucketNames.length > 0;
        if (Objects.isNull(value) || value.isNaN()) {
            return !isNamePresent ? bucketNameInitial + "NULL" : bucketNames[0];
        }
        if (value.isInfinite()) {
            return value < 0
                    ? !isNamePresent ? bucketNameInitial + 1 : bucketNames[1]
                    : !isNamePresent
                            ? bucketNameInitial + (splitBuckets.length + 1)
                            : bucketNames[bucketNames.length - 1];
        }
        return mapFiniteValueToBucket(value, splitBuckets, bucketNames);
    }

    private String mapLongValueToBucket(Long value, Long[] splitBuckets, String[] bucketNames) {
        boolean isNamePresent = bucketNames.length > 0;
        if (Objects.isNull(value)) {
            return !isNamePresent ? bucketNameInitial + "NULL" : bucketNames[0];
        }
        return mapFiniteValueToBucket(value, splitBuckets, bucketNames);
    }

    private <T extends Comparable<T>> String mapFiniteValueToBucket(
            T value, T[] splitBuckets, String[] bucketNames) {
        Arrays.sort(splitBuckets);
        boolean isNamePresent = bucketNames.length > 0;
        for (int i = 0; i < splitBuckets.length; i++) {
            if (value.compareTo(splitBuckets[i]) < 0) {
                return !isNamePresent ? bucketNameInitial + (i + 1) : bucketNames[i + 1];
            }
        }
        return !isNamePresent
                ? bucketNameInitial + (splitBuckets.length + 1)
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
                                                            if (argDataType
                                                                            instanceof
                                                                            AtomicDataType
                                                                    && !argDataType
                                                                            .getLogicalType()
                                                                            .isNullable()) {
                                                                argDataType =
                                                                        argDataType.nullable();
                                                            }
                                                            switch (i) {
                                                                case (0):
                                                                    if (!DATA_TYPE_SET.contains(
                                                                                    argDataType)
                                                                            && !argDataType
                                                                                    .toString()
                                                                                    .startsWith(
                                                                                            "DECIMAL")
                                                                            && !argDataType
                                                                                    .toString()
                                                                                    .startsWith(
                                                                                            "TIME")
                                                                            && !argDataType
                                                                                    .toString()
                                                                                    .startsWith(
                                                                                            "INTERVAL")) {
                                                                        return String.format(
                                                                                "The %s data type is not supported as the first argument to the %s function.",
                                                                                argDataType, NAME);
                                                                    }
                                                                    break;
                                                                case (1):
                                                                    if (argDataType
                                                                            .toString()
                                                                            .startsWith("ARRAY")) {
                                                                        DataType childDataType =
                                                                                argDataType
                                                                                        .getChildren()
                                                                                        .get(0);
                                                                        if (childDataType
                                                                                        instanceof
                                                                                        AtomicDataType
                                                                                && !childDataType
                                                                                        .getLogicalType()
                                                                                        .isNullable()) {
                                                                            childDataType =
                                                                                    childDataType
                                                                                            .nullable();
                                                                        }
                                                                        if (!DATA_TYPE_SET.contains(
                                                                                        childDataType)
                                                                                && !childDataType
                                                                                        .toString()
                                                                                        .startsWith(
                                                                                                "DECIMAL")
                                                                                && !childDataType
                                                                                        .toString()
                                                                                        .startsWith(
                                                                                                "TIME")
                                                                                && !childDataType
                                                                                        .toString()
                                                                                        .startsWith(
                                                                                                "INTERVAL")) {
                                                                            return String.format(
                                                                                    "Arrays of type %s are not supported as the second argument to the %s function.",
                                                                                    childDataType,
                                                                                    NAME);
                                                                        }
                                                                    } else {
                                                                        return String.format(
                                                                                "The second argument to the %s function must be of ARRAY data type.",
                                                                                NAME);
                                                                    }
                                                                    break;
                                                                case (2):
                                                                    if (argDataType
                                                                            .toString()
                                                                            .startsWith("ARRAY")) {
                                                                        DataType childDataType =
                                                                                argDataType
                                                                                        .getChildren()
                                                                                        .get(0);
                                                                        if (childDataType
                                                                                        instanceof
                                                                                        AtomicDataType
                                                                                && !childDataType
                                                                                        .getLogicalType()
                                                                                        .isNullable()) {
                                                                            childDataType =
                                                                                    childDataType
                                                                                            .nullable();
                                                                        }
                                                                        if (!childDataType.equals(
                                                                                        DataTypes
                                                                                                .STRING())
                                                                                && !childDataType
                                                                                        .toString()
                                                                                        .startsWith(
                                                                                                "CHAR")
                                                                                && !childDataType
                                                                                        .toString()
                                                                                        .startsWith(
                                                                                                "VARCHAR")) {
                                                                            return String.format(
                                                                                    "Arrays of type %s are not supported as the third argument to the %s function. "
                                                                                            + "Please provide Array of type STRING with valid names.",
                                                                                    childDataType,
                                                                                    NAME);
                                                                        }
                                                                    } else {
                                                                        return String.format(
                                                                                "The third argument to the %s function must be of ARRAY data type.",
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
    private <T extends Number> T[] castToNumberArray(Object arrayObject, Class<T> targetType) {
        int length = Array.getLength(arrayObject);
        List<T> numberList = new ArrayList<>();
        for (int i = 0; i < length; i++) {
            if (Objects.isNull(Array.get(arrayObject, i))) {
                continue;
            }
            Object element = Array.get(arrayObject, i);
            if (targetType.equals(Double.class)) {
                Double doubleValue = MlFunctionsUtil.getDoubleValue(element, NAME);
                if (doubleValue.isNaN()) {
                    continue;
                }
                numberList.add((T) doubleValue);
            } else if (targetType.equals(Long.class)) {
                numberList.add((T) MlFunctionsUtil.getLongValue(element, NAME));
            } else {
                throw new FlinkRuntimeException(
                        String.format(
                                "The %s function does not support arrays of type %s as arguments.",
                                NAME, targetType.getSimpleName()));
            }
        }
        return numberList.toArray((T[]) Array.newInstance(targetType, 0));
    }

    private <T extends Comparable<T>> void validateBucketNames(
            T[] splitBuckets, String[] bucketNames) {
        if (bucketNames.length != splitBuckets.length + 2) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Invalid number of bucket names passed to the function %s. "
                                    + "Please provide names for all bucket names excluding undefined buckets and including first name for undefined values",
                            NAME));
        }
        if (!isArraySorted(splitBuckets)) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Invalid order of buckets passed to the function %s. "
                                    + "Buckets must be sorted in ascending order when passed with bucket names.",
                            NAME));
        }
    }

    private <T extends Comparable<T>> boolean isArraySorted(T[] splitBuckets) {
        for (int i = 0; i < splitBuckets.length - 1; ++i) {
            if (splitBuckets[i].compareTo(splitBuckets[i + 1]) > 0) return false;
        }
        return true;
    }
}
