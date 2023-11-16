/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Specification of a remote UDF. */
public class RemoteUdfSpec implements Serializable {
    private static final long serialVersionUID = 1L;

    /** ID under which the UDF was registered at creation time. */
    private final String functionId;

    /** Name of the class that contains that implements the UDF. */
    private final String functionClassName;

    /** DataType for the return value of the UDF call. */
    private final DataType returnType;

    /** DataTypes for the arguments of the UDF call. */
    private final List<DataType> argumentTypes;

    public RemoteUdfSpec(
            String functionId,
            String functionClassName,
            DataType returnType,
            List<DataType> argumentTypes) {
        this.functionId = functionId;
        this.functionClassName = functionClassName;
        this.returnType = returnType;
        this.argumentTypes = argumentTypes;
    }

    public String getFunctionId() {
        return functionId;
    }

    public String getFunctionClassName() {
        return functionClassName;
    }

    public DataType getReturnType() {
        return returnType;
    }

    public List<DataType> getArgumentTypes() {
        return argumentTypes;
    }

    /** Creates the serializer for the return value. */
    public TypeSerializer<Object> createReturnTypeSerializer() {
        return serializerForDataType(returnType);
    }

    /** Creates a list of serializers for the argument values. */
    public List<TypeSerializer<Object>> createArgumentSerializers() {
        return argumentTypes.stream()
                .map(RemoteUdfSpec::serializerForDataType)
                .collect(Collectors.toList());
    }

    /**
     * Writes this instance to the given output, e.g. to share the spec with the remote service when
     * the function is opened, and we communicate the types.
     *
     * @param out the output to write into.
     * @throws IOException on serialization errors.
     */
    public void serialize(DataOutput out) throws IOException {
        out.writeUTF(functionId);
        out.writeUTF(functionClassName);
        out.writeUTF(stringifyDataType(returnType));
        out.writeInt(argumentTypes.size());
        for (DataType argumentType : argumentTypes) {
            out.writeUTF(stringifyDataType(argumentType));
        }
    }

    /**
     * Deserializes a spec from the given input.
     *
     * @param in the input to read from.
     * @param classLoader the classloader to use during deserialization.
     * @return the deserialized specs.
     * @throws IOException on deserialization errors.
     */
    public static RemoteUdfSpec deserialize(DataInput in, ClassLoader classLoader)
            throws IOException {
        String functionId = in.readUTF();
        String functionClassName = in.readUTF();
        DataType returnType = parseFromString(in.readUTF(), classLoader);
        int numArgs = in.readInt();
        List<DataType> argumentTypes = new ArrayList<>(numArgs);
        for (int i = 0; i < numArgs; ++i) {
            argumentTypes.add(parseFromString(in.readUTF(), classLoader));
        }
        return new RemoteUdfSpec(functionId, functionClassName, returnType, argumentTypes);
    }

    private static String stringifyDataType(DataType dataType) {
        LogicalType logicalType = dataType.getLogicalType();
        return logicalType.asSerializableString();
    }

    private static DataType parseFromString(String s, ClassLoader classLoader) {
        return DataTypeUtils.toInternalDataType(LogicalTypeParser.parse(s, classLoader));
    }

    private static TypeSerializer<Object> serializerForDataType(DataType dataType) {
        LogicalType logicalType = dataType.getLogicalType();
        return InternalSerializers.create(logicalType);
    }
}
