/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.avro;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.flink.formats.registry.SchemaRegistryCoder;
import io.confluent.flink.formats.registry.SchemaRegistryConfig;
import io.confluent.flink.formats.registry.avro.converters.AvroToRowDataConverters;
import io.confluent.flink.formats.registry.avro.converters.AvroToRowDataConverters.AvroToRowDataConverter;
import io.confluent.flink.formats.registry.utils.MutableByteArrayInputStream;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link DeserializationSchema} that deserializes {@link RowData} from Avro records using Schema
 * Registry protocol.
 */
@Confluent
public class AvroRegistryDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SchemaRegistryConfig schemaRegistryConfig;

    private final RowType rowType;
    private final TypeInformation<RowData> producedType;

    //
    // Initialized in open()
    //
    /** Runtime instance that performs the actual work. */
    private transient AvroToRowDataConverter runtimeConverter;

    /** Reader that reads the serialized record from {@link MutableByteArrayInputStream}. */
    private transient GenericDatumReader<Object> datumReader;

    /** Input stream to read message from. */
    private transient MutableByteArrayInputStream inputStream;

    /** Avro decoder that decodes binary data. */
    private transient Decoder decoder;

    private transient SchemaRegistryCoder schemaCoder;

    /**
     * Creates an Avro deserialization schema for the given logical type.
     *
     * @param schemaRegistryConfig configuration how to access Schema Registry
     * @param rowType the type of the row that is consumed
     */
    public AvroRegistryDeserializationSchema(
            SchemaRegistryConfig schemaRegistryConfig,
            RowType rowType,
            TypeInformation<RowData> typeInformation) {
        this.schemaRegistryConfig = schemaRegistryConfig;
        this.rowType = rowType;
        this.producedType = typeInformation;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        final SchemaRegistryClient schemaRegistryClient =
                schemaRegistryConfig.createClient(context.getJobID().orElse(null));
        final ParsedSchema schema =
                schemaRegistryClient.getSchemaById(schemaRegistryConfig.getSchemaId());
        final Schema avroSchema = (Schema) schema.rawSchema();

        this.runtimeConverter =
                avroSchema.getType() == Type.RECORD || avroSchema.getType() == Type.UNION
                        // RECORD and UNION get converted to ROW
                        ? AvroToRowDataConverters.createConverter(avroSchema, rowType)
                        : createSingleFieldConverter(avroSchema);

        this.datumReader =
                new GenericDatumReader<>(
                        avroSchema,
                        avroSchema,
                        new GenericData(Thread.currentThread().getContextClassLoader()));
        this.inputStream = new MutableByteArrayInputStream();
        this.decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        this.schemaCoder =
                new SchemaRegistryCoder(schemaRegistryConfig.getSchemaId(), schemaRegistryClient);
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            inputStream.setBuffer(message);
            Schema writerSchema = (Schema) schemaCoder.readSchema(inputStream).rawSchema();

            datumReader.setSchema(writerSchema);
            Object deserialize = datumReader.read(null, decoder);
            return (RowData) runtimeConverter.convert(deserialize);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Avro record.", e);
        }
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvroRegistryDeserializationSchema that = (AvroRegistryDeserializationSchema) o;
        return Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig)
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(producedType, that.producedType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaRegistryConfig, rowType, producedType);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    private AvroToRowDataConverter createSingleFieldConverter(Schema avroSchema) {
        AvroToRowDataConverter fieldConverter =
                AvroToRowDataConverters.createConverter(avroSchema, rowType.getTypeAt(0));
        return object -> {
            GenericRowData rowData = new GenericRowData(1);
            rowData.setField(0, fieldConverter.convert(object));
            return rowData;
        };
    }
}
