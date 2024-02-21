/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.codegen;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.typeutils.LogicalRelDataTypeConverter;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalType;

import io.confluent.flink.udf.adapter.ScalarFunctionInstanceCallAdapter;
import io.confluent.flink.udf.adapter.extraction.ExtractorDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utility class to generate adapter code for calling ScalarFunctions. */
public final class ScalarFunctionAdapterGenerator {

    /**
     * Generates an adapter which can invoke the given udf.
     *
     * @param callerUUID The uuid of the caller
     * @param instance Instance of a ScalarFunction
     * @param argumentTypes The argument types
     * @param returnType The return type
     * @param classLoader The classloader
     * @return The adapter to issue calls
     */
    public static ScalarFunctionInstanceCallAdapter generate(
            String callerUUID,
            ScalarFunction instance,
            List<DataType> argumentTypes,
            DataType returnType,
            ClassLoader classLoader) {
        final String adapterName = "Udf_Adapter_" + callerUUID.replace('-', '_');
        final Configuration tableConfig = getTableConfig();
        LogicalType returnLogicalType = returnType.getLogicalType();
        List<LogicalType> argumentLogicalTypes =
                argumentTypes.stream().map(DataType::getLogicalType).collect(Collectors.toList());
        final RexNode callNode =
                createCallNode(
                        instance, classLoader, argumentLogicalTypes, argumentTypes, returnType);

        final GeneratedFunction<RichMapFunction<RowData, Object>> generatedFunction =
                ScalarFunctionCodeGenerator.generate(
                        tableConfig,
                        adapterName,
                        returnLogicalType,
                        argumentLogicalTypes,
                        classLoader,
                        callNode);

        final RichMapFunction<RowData, Object> function =
                generatedFunction.newInstance(classLoader);
        return new ScalarFunctionInstanceCallAdapter(function, tableConfig);
    }

    /**
     * Creates a {@link RexNode} used for code generation. This is almost a simplified version of
     * the parser, but is obviously much simpler.
     */
    private static RexNode createCallNode(
            ScalarFunction instance,
            ClassLoader classLoader,
            List<LogicalType> argumentLogicalTypes,
            List<DataType> argumentTypes,
            DataType returnType) {
        final FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(classLoader, FlinkTypeSystem.INSTANCE);
        final RexBuilder builder = new FlinkRexBuilder(typeFactory);
        final ExtractorDataTypeFactory dataTypeFactory = new ExtractorDataTypeFactory(classLoader);
        final List<RexNode> inputRefs =
                IntStream.range(0, argumentLogicalTypes.size())
                        .mapToObj(
                                i -> {
                                    LogicalType logicalType = argumentLogicalTypes.get(i);
                                    return new RexInputRef(
                                            i,
                                            LogicalRelDataTypeConverter.toRelDataType(
                                                    logicalType, typeFactory));
                                })
                        .collect(Collectors.toList());
        return builder.makeCall(
                BridgingSqlFunction.of(
                        dataTypeFactory,
                        typeFactory,
                        null,
                        SqlKind.OTHER_FUNCTION,
                        ContextResolvedFunction.anonymous(instance),
                        // It's important we say that the type inference is that of what we expect,
                        // Otherwise, it won't catch if there's a mismatch
                        getTypeInference(argumentTypes, returnType)),
                inputRefs);
    }

    private static TypeInference getTypeInference(
            List<DataType> argumentTypes, DataType returnType) {
        TypeInference.Builder builder = TypeInference.newBuilder();
        builder.typedArguments(argumentTypes);
        builder.outputTypeStrategy(TypeStrategies.explicit(returnType));
        return builder.build();
    }

    private static Configuration getTableConfig() {
        // These can potentially be populated with values sent from Flink.
        return new Configuration();
    }
}
