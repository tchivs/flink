/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.avro;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.flink.formats.registry.SchemaRegistryCoder;
import io.confluent.flink.formats.registry.SchemaRegistryConfig;
import io.confluent.flink.formats.registry.avro.converters.RowDataToAvroConverters;
import io.confluent.flink.formats.registry.avro.converters.RowDataToAvroConverters.RowDataToAvroConverter;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

/** A {@link SerializationSchema} that serializes {@link RowData} using Schema Registry protocol. */
@Confluent
public class AvroRegistrySerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SchemaRegistryConfig schemaRegistryConfig;

    private final RowType rowType;

    //
    // Initialized in open()
    //
    /** Runtime instance that performs the actual work. */
    private transient RowDataToAvroConverter runtimeConverter;

    /** Writer that writes the serialized record to {@link ByteArrayOutputStream}. */
    private transient GenericDatumWriter<Object> datumWriter;

    /** Output stream to write message to. */
    private transient ByteArrayOutputStream arrayOutputStream;

    /** Avro encoder that encodes binary data. */
    private transient BinaryEncoder encoder;

    private transient SchemaRegistryCoder schemaCoder;

    /**
     * Creates an Avro deserialization schema for the given logical type.
     *
     * @param schemaRegistryConfig configuration how to access Schema Registry
     * @param rowType the type of the row that is consumed
     */
    public AvroRegistrySerializationSchema(
            SchemaRegistryConfig schemaRegistryConfig, RowType rowType) {
        this.schemaRegistryConfig = schemaRegistryConfig;
        this.rowType = rowType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        final SchemaRegistryClient schemaRegistryClient =
                schemaRegistryConfig.createClient(context.getJobID().orElse(null));
        final ParsedSchema schema =
                schemaRegistryClient.getSchemaById(schemaRegistryConfig.getSchemaId());
        final Schema avroSchema = (Schema) schema.rawSchema();
        if (avroSchema.getType() == Type.RECORD) {
            this.runtimeConverter = RowDataToAvroConverters.createConverter(rowType, avroSchema);
        } else {
            final LogicalType fieldType = rowType.getTypeAt(0);
            final RowDataToAvroConverter fieldConverter =
                    RowDataToAvroConverters.createConverter(fieldType, avroSchema);
            final FieldGetter fieldGetter = RowData.createFieldGetter(fieldType, 0);
            this.runtimeConverter =
                    object -> {
                        final RowData rowData = (RowData) object;
                        return fieldConverter.convert(fieldGetter.getFieldOrNull(rowData));
                    };
        }

        this.datumWriter =
                new GenericDatumWriter<>(
                        avroSchema,
                        new GenericData(Thread.currentThread().getContextClassLoader()));
        this.arrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().directBinaryEncoder(arrayOutputStream, null);
        this.schemaCoder =
                new SchemaRegistryCoder(schemaRegistryConfig.getSchemaId(), schemaRegistryClient);
    }

    @Override
    public byte[] serialize(RowData row) {
        try {
            if (row == null) {
                return null;
            } else {
                // convert to record
                final Object record = runtimeConverter.convert(row);
                arrayOutputStream.reset();
                schemaCoder.writeSchema(arrayOutputStream);
                datumWriter.write(record, encoder);
                encoder.flush();
                return arrayOutputStream.toByteArray();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize row.", e);
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
        AvroRegistrySerializationSchema that = (AvroRegistrySerializationSchema) o;
        return Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig)
                && Objects.equals(rowType, that.rowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaRegistryConfig, rowType);
    }
}
