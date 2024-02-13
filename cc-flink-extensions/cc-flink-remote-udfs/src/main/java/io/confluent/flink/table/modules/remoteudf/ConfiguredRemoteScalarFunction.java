/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class which parses configs to return a list of remote Udfs which can be used for planning. It's
 * also a specialized function which also returns a {@link RemoteScalarFunction} implementation
 * initialized with a function spec for the remote call.
 */
public class ConfiguredRemoteScalarFunction extends UserDefinedFunction
        implements SpecializedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(ConfiguredRemoteScalarFunction.class);

    private final Map<String, String> config;
    private final List<ConfiguredFunctionSpec> configuredFunctionSpecs;

    // Useful for the planner so that it does't need to provide a config which is only required
    // during runtime
    public ConfiguredRemoteScalarFunction(List<ConfiguredFunctionSpec> configuredFunctionSpecs) {
        this(Collections.emptyMap(), configuredFunctionSpecs);
    }

    // If used at runtime, must provide a config
    public ConfiguredRemoteScalarFunction(
            Map<String, String> config, List<ConfiguredFunctionSpec> configuredFunctionSpecs) {
        this.config = config;
        this.configuredFunctionSpecs = configuredFunctionSpecs;
        LOG.info("ConfiguredRemoteScalarFunction config: {}", config);
    }

    public String getFunctionName() {
        Preconditions.checkState(!configuredFunctionSpecs.isEmpty());
        return configuredFunctionSpecs.get(0).getName();
    }

    public String getFunctionCatalog() {
        Preconditions.checkState(!configuredFunctionSpecs.isEmpty());
        return configuredFunctionSpecs.get(0).getCatalog();
    }

    public String getFunctionDatabase() {
        Preconditions.checkState(!configuredFunctionSpecs.isEmpty());
        return configuredFunctionSpecs.get(0).getDatabase();
    }

    public List<ConfiguredFunctionSpec> getConfiguredFunctionSpecs() {
        return configuredFunctionSpecs;
    }

    public String getPath() {
        return getFunctionCatalog() + "." + getFunctionDatabase() + "." + getFunctionName();
    }

    @Override
    public FunctionKind getKind() {
        return FunctionKind.SCALAR;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        Map<InputTypeStrategy, TypeStrategy> mapping = new HashMap<>();
        for (ConfiguredFunctionSpec spec : configuredFunctionSpecs) {
            mapping.put(
                    InputTypeStrategies.explicitSequence(
                            spec.getArgumentTypes(typeFactory).toArray(new DataType[0])),
                    TypeStrategies.explicit(spec.getReturnType(typeFactory)));
        }
        TypeInference.Builder builder = TypeInference.newBuilder();
        builder.inputTypeStrategy(
                        InputTypeStrategies.or(mapping.keySet().toArray(new InputTypeStrategy[0])))
                .outputTypeStrategy(TypeStrategies.mapping(mapping));
        return builder.build();
    }

    @Override
    public UserDefinedFunction specialize(SpecializedContext context) {
        DataTypeFactory typeFactory = context.getCallContext().getDataTypeFactory();
        for (ConfiguredFunctionSpec spec : configuredFunctionSpecs) {
            if (isCompatible(
                            context.getCallContext().getArgumentDataTypes(),
                            spec.getArgumentTypes(typeFactory))
                    && context.getCallContext().getOutputDataType().isPresent()
                    && isCompatible(
                            context.getCallContext().getOutputDataType().get(),
                            spec.getReturnType(typeFactory))) {
                final RemoteUdfSpec remoteUdfSpec = spec.createRemoteUdfSpec(typeFactory);
                return RemoteScalarFunction.create(config, remoteUdfSpec);
            }
        }
        throw new FlinkRuntimeException("Cannot find method signature match");
    }

    private boolean isCompatible(DataType callType, DataType specType) {
        return LogicalTypeCasts.supportsAvoidingCast(
                callType.getLogicalType(), specType.getLogicalType());
    }

    private boolean isCompatible(List<DataType> callArgs, List<DataType> specArgs) {
        if (callArgs.size() != specArgs.size()) {
            return false;
        }
        for (int i = 0; i < callArgs.size(); i++) {
            DataType callArg = callArgs.get(i);
            DataType specArg = specArgs.get(i);
            if (!isCompatible(callArg, specArg)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
