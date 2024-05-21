/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.confluent.flink.formats.registry.SchemaRegistryCoder;
import io.confluent.flink.formats.registry.SchemaRegistryConfig;
import io.confluent.flink.formats.registry.json.JsonToRowDataConverters.JsonToRowDataConverter;
import io.confluent.flink.formats.registry.utils.MutableByteArrayInputStream;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.everit.json.schema.Schema;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link DeserializationSchema} that deserializes {@link RowData} from JSON records using Schema
 * Registry protocol.
 */
public class JsonRegistryDeserializationSchema implements DeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final SchemaRegistryConfig schemaRegistryConfig;
    private final RowType rowType;
    private final TypeInformation<RowData> producedType;

    /** Input stream to read message from. */
    private transient MutableByteArrayInputStream inputStream;

    private transient SchemaRegistryCoder schemaCoder;

    private transient ObjectMapper objectMapper;

    private transient JsonToRowDataConverter runtimeConverter;

    public JsonRegistryDeserializationSchema(
            SchemaRegistryConfig schemaRegistryConfig,
            RowType rowType,
            TypeInformation<RowData> producedType) {
        this.schemaRegistryConfig = schemaRegistryConfig;
        this.rowType = rowType;
        this.producedType = producedType;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        final SchemaRegistryClient schemaRegistryClient =
                schemaRegistryConfig.createClient(context.getJobID().orElse(null));
        this.schemaCoder =
                new SchemaRegistryCoder(schemaRegistryConfig.getSchemaId(), schemaRegistryClient);
        final ParsedSchema schema =
                schemaRegistryClient.getSchemaById(schemaRegistryConfig.getSchemaId());
        final Schema jsonSchema = (Schema) schema.rawSchema();
        this.runtimeConverter = JsonToRowDataConverters.createConverter(jsonSchema, rowType);
        this.inputStream = new MutableByteArrayInputStream();
        this.objectMapper =
                new ObjectMapper()
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            inputStream.setBuffer(message);
            schemaCoder.readSchema(inputStream);

            final JsonNode jsonNode = objectMapper.readTree(inputStream);
            return (RowData) runtimeConverter.convert(jsonNode);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize JSON record.", e);
        }
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
        JsonRegistryDeserializationSchema that = (JsonRegistryDeserializationSchema) o;
        return Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig)
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(producedType, that.producedType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaRegistryConfig, rowType, producedType);
    }
}
