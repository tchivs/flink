/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.protobuf;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.DynamicMessage;
import io.confluent.flink.formats.registry.SchemaRegistryCoder;
import io.confluent.flink.formats.registry.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink internal data structure with a Protobuf
 * format.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * ProtoRegistryDeserializationSchema}.
 */
@Confluent
public class ProtoRegistrySerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** RowType to generate the runtime converter. */
    private final RowType rowType;

    private final SchemaRegistryConfig schemaRegistryConfig;

    /** The converter that converts internal data formats to JsonNode. */
    private transient RowDataToProtoConverters.RowDataToProtoConverter runtimeConverter;

    private transient SchemaRegistryCoder schemaCoder;
    /** Output stream to write message to. */
    private transient ByteArrayOutputStream arrayOutputStream;

    public ProtoRegistrySerializationSchema(SchemaRegistryConfig registryConfig, RowType rowType) {
        this.rowType = Preconditions.checkNotNull(rowType);
        this.schemaRegistryConfig = Preconditions.checkNotNull(registryConfig);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        final SchemaRegistryClient schemaRegistryClient =
                schemaRegistryConfig.createClient(context.getJobID().orElse(null));
        this.schemaCoder =
                new SchemaRegistryCoder(schemaRegistryConfig.getSchemaId(), schemaRegistryClient);
        final ProtobufSchema schema =
                (ProtobufSchema)
                        schemaRegistryClient.getSchemaById(schemaRegistryConfig.getSchemaId());
        this.runtimeConverter =
                RowDataToProtoConverters.createConverter(rowType, schema.toDescriptor());
        this.arrayOutputStream = new ByteArrayOutputStream();
    }

    @Override
    public byte[] serialize(RowData row) {
        try {
            final DynamicMessage converted = (DynamicMessage) runtimeConverter.convert(row);

            arrayOutputStream.reset();
            schemaCoder.writeSchema(arrayOutputStream);
            converted.writeTo(arrayOutputStream);
            return arrayOutputStream.toByteArray();
        } catch (Throwable t) {
            throw new FlinkRuntimeException(String.format("Could not serialize row '%s'.", row), t);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProtoRegistrySerializationSchema that = (ProtoRegistrySerializationSchema) o;
        return Objects.equals(rowType, that.rowType)
                && Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, schemaRegistryConfig);
    }
}
