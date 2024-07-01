/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.protobuf;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.flink.formats.registry.SchemaRegistryCoder;
import io.confluent.flink.formats.registry.SchemaRegistryConfig;
import io.confluent.flink.formats.registry.protobuf.ProtoToRowDataConverters.ProtoToRowDataConverter;
import io.confluent.flink.formats.registry.utils.MutableByteArrayInputStream;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link DeserializationSchema} that deserializes {@link RowData} from Protobuf messages using
 * Schema Registry protocol.
 */
@Confluent
public class ProtoRegistryDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SchemaRegistryConfig schemaRegistryConfig;
    private final RowType rowType;
    private final TypeInformation<RowData> producedType;

    /** Input stream to read message from. */
    private transient MutableByteArrayInputStream inputStream;

    private transient SchemaRegistryCoder schemaCoder;

    private transient ProtoToRowDataConverter[] runtimeConverters;
    private transient List<Descriptor> messageTypes;

    public ProtoRegistryDeserializationSchema(
            SchemaRegistryConfig schemaRegistryConfig,
            RowType rowType,
            TypeInformation<RowData> producedType) {
        this.schemaRegistryConfig = Preconditions.checkNotNull(schemaRegistryConfig);
        this.rowType = Preconditions.checkNotNull(rowType);
        this.producedType = Preconditions.checkNotNull(producedType);
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
        this.messageTypes = schema.toDescriptor().getFile().getMessageTypes();
        this.runtimeConverters = createConverters(this.messageTypes);
        this.inputStream = new MutableByteArrayInputStream();
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }

        // Kafka Wire Format: magic-byte, schema-id, message-indexes, protobuf-payload
        try {
            inputStream.setBuffer(message);
            schemaCoder.readSchema(inputStream); // <- magic-byte + schema-id
            final Descriptor descriptor = readDescriptor(inputStream); //   <- message-indexes
            final DynamicMessage dynamicMessage =
                    DynamicMessage.parseFrom(descriptor, inputStream); //   <- protobuf-payload
            return (RowData) runtimeConverters[descriptor.getIndex()].convert(dynamicMessage);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Protobuf message.", e);
        }
    }

    /**
     * Read the message indexes from the input stream and use it to get the descriptor used to
     * serialize the message. The message indexes are encoded as int using variable-length zig-zag
     * encoding, prefixed by the length of the array. See the protobuf section under
     * https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
     * We only support top level schema, so we only need the first message index. Multiple message
     * indexes indicative of nested schema serialization are not supported and stop the Flink Job.
     *
     * @param inputStream The input stream carrying the message in kafka's protobuf wire format.
     */
    private Descriptor readDescriptor(MutableByteArrayInputStream inputStream) throws IOException {
        final DataInputStream dataInputStream = new DataInputStream(inputStream);

        // read the message indexes array size
        final int messageIndexesArraySize = ByteUtils.readVarint(dataInputStream);

        if (messageIndexesArraySize > 1) {
            // payload was serialized with nested schema
            throw new TableRuntimeException(
                    "Payload could not be deserialized. "
                            + "The 'protobuf-registry' format only supports deserialization of messages that have been defined as top-level messages in the schema. "
                            + "Nested messages as top-level messages is not supported.");
        }

        if (messageIndexesArraySize == 0) {
            // no message indexes => single message in schema, use the first and only message type
            return messageTypes.get(0);
        }

        // since there is only one element in the message indexes length,
        // the next readVarint will return the message index of the message type
        final int messageIndex = ByteUtils.readVarint(dataInputStream);
        return messageTypes.get(messageIndex);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProtoRegistryDeserializationSchema that = (ProtoRegistryDeserializationSchema) o;
        return Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig)
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(producedType, that.producedType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaRegistryConfig, rowType, producedType);
    }

    /**
     * Single message => create converter from [first and only message type -> top-level row type].
     * Multiple messages => disaggregate the row into its children rows and create an array of
     * converters for every [message type -> child row] pair
     *
     * @return an array of converters
     */
    private ProtoToRowDataConverter[] createConverters(final List<Descriptor> messageTypes) {
        final int arity = messageTypes.size();
        final RowType[] rowTypes =
                arity == 1
                        ? new RowType[] {rowType}
                        : rowType.getChildren().toArray(RowType[]::new);

        final ProtoToRowDataConverter[] converters = new ProtoToRowDataConverter[arity];
        for (Descriptor messageType : messageTypes) {
            final int descriptorIndex = messageType.getIndex();
            final RowType targetRowType = rowTypes[descriptorIndex];
            converters[descriptorIndex] =
                    ProtoToRowDataConverters.createConverter(messageType, targetRowType);
        }
        return converters;
    }
}
