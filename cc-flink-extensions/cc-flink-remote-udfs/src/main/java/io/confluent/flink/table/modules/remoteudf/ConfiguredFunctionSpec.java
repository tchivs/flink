/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A class representing a function spec, retrieved from a config. */
public class ConfiguredFunctionSpec implements Serializable {

    private static final long serialVersionUID = -6590842532566680559L;
    private static final String ARGUMENT_TYPE_DELIMITER = ";";
    private final String argumentTypes;
    private final String returnType;
    private final String organization;
    private final String environment;
    private final String pluginId;
    private final String pluginVersionId;
    private final String className;
    private final boolean isDeterministic;
    private final String catalog;
    private final String database;
    private final String name;

    private ConfiguredFunctionSpec(
            String organization,
            String environment,
            String catalog,
            String database,
            String name,
            String argumentTypes,
            String returnType,
            String pluginId,
            String pluginVersionId,
            String className,
            boolean isDeterministic) {
        this.organization = organization;
        this.environment = environment;
        this.catalog = catalog;
        this.database = database;
        this.name = name;
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
        this.pluginId = pluginId;
        this.pluginVersionId = pluginVersionId;
        this.className = className;
        this.isDeterministic = isDeterministic;
    }

    public RemoteUdfSpec createRemoteUdfSpec(DataTypeFactory typeFactory) {
        return new RemoteUdfSpec(
                organization,
                environment,
                pluginId,
                pluginVersionId,
                className,
                isDeterministic,
                getReturnType(typeFactory),
                getArgumentTypes(typeFactory));
    }

    public List<DataType> getArgumentTypes(DataTypeFactory typeFactory) {
        DataType[] inputTypes =
                Arrays.stream(argumentTypes.split(ARGUMENT_TYPE_DELIMITER))
                        .filter(strType -> !strType.isEmpty())
                        .map(typeFactory::createDataType)
                        .toArray(DataType[]::new);
        return Stream.of(inputTypes).collect(Collectors.toList());
    }

    public String getArgumentTypes() {
        return argumentTypes;
    }

    public DataType getReturnType(DataTypeFactory typeFactory) {
        return typeFactory.createDataType(returnType);
    }

    public String getReturnType() {
        return returnType;
    }

    public String getOrganization() {
        return organization;
    }

    public String getEnvironment() {
        return environment;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }

    public String getPluginId() {
        return pluginId;
    }

    public String getPluginVersionId() {
        return pluginVersionId;
    }

    public String getClassName() {
        return className;
    }

    public boolean isDeterministic() {
        return isDeterministic;
    }

    public String getIsDeterministicAsString() {
        return Boolean.toString(isDeterministic);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** A builder for {@link ConfiguredFunctionSpec}. */
    public static class Builder {
        private String organization;
        private String environment;
        private String catalog;
        private String database;
        private String name;
        private final List<String> argumentTypes = new ArrayList<>();
        private final List<String> returnTypes = new ArrayList<>();
        private String pluginId;
        private String pluginVersionId;
        private String className;
        private Boolean isDeterministic;

        public Builder addArgumentTypes(String argumentTypes) {
            this.argumentTypes.add(argumentTypes);
            return this;
        }

        public Builder addArgumentTypes(List<String> argumentTypes) {
            this.argumentTypes.add(String.join(ARGUMENT_TYPE_DELIMITER, argumentTypes));
            return this;
        }

        public Builder addReturnType(String returnType) {
            this.returnTypes.add(returnType);
            return this;
        }

        public Builder setOrganization(String organization) {
            this.organization = organization;
            return this;
        }

        public Builder setEnvironment(String environment) {
            this.environment = environment;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setCatalog(String catalog) {
            this.catalog = catalog;
            this.name = name;
            return this;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setPluginId(String pluginId) {
            this.pluginId = pluginId;
            return this;
        }

        public Builder setPluginVersionId(String pluginVersionId) {
            this.pluginVersionId = pluginVersionId;
            return this;
        }

        public Builder setClassName(String className) {
            this.className = className;
            return this;
        }

        public Builder parseIsDeterministic(String isDeterministic) {
            this.isDeterministic = Boolean.parseBoolean(isDeterministic);
            return this;
        }

        public List<ConfiguredFunctionSpec> build() {
            Preconditions.checkNotNull(organization);
            Preconditions.checkNotNull(environment);
            Preconditions.checkNotNull(database);
            Preconditions.checkNotNull(catalog);
            Preconditions.checkNotNull(database);
            Preconditions.checkNotNull(name);
            Preconditions.checkNotNull(argumentTypes);
            Preconditions.checkNotNull(returnTypes);
            Preconditions.checkNotNull(pluginId);
            Preconditions.checkNotNull(pluginVersionId);
            Preconditions.checkNotNull(className);
            Preconditions.checkNotNull(isDeterministic);
            Preconditions.checkState(
                    argumentTypes.size() == returnTypes.size(),
                    "Args and results should be equal in size");
            List<ConfiguredFunctionSpec> result = new ArrayList<>();
            for (int i = 0; i < returnTypes.size(); i++) {
                result.add(
                        new ConfiguredFunctionSpec(
                                organization,
                                environment,
                                catalog,
                                database,
                                name,
                                argumentTypes.get(i),
                                returnTypes.get(i),
                                pluginId,
                                pluginVersionId,
                                className,
                                isDeterministic));
            }
            return result;
        }
    }
}
