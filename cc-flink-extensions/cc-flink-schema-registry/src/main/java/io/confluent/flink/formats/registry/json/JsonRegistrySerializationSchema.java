/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.flink.formats.registry.SchemaRegistryCoder;
import io.confluent.flink.formats.registry.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.jackson.JsonOrgModule;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink internal data structure into a JSON
 * bytes.
 *
 * <p>Serializes the input Flink object into a JSON string and converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link
 * JsonRegistryDeserializationSchema}.
 */
@Confluent
public class JsonRegistrySerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** RowType to generate the runtime converter. */
    private final RowType rowType;

    private final SchemaRegistryConfig schemaRegistryConfig;

    private final ValidateMode validateBeforeWrite;

    /** The converter that converts internal data formats to JsonNode. */
    private transient RowDataToJsonConverters.RowDataToJsonConverter runtimeConverter;

    /** Object mapper that is used to create output JSON objects. */
    private transient ObjectMapper objectMapper;

    /** Reusable object node. */
    private transient ObjectNode node;

    private transient SchemaRegistryCoder schemaCoder;
    private transient Schema jsonSchema;
    /** Output stream to write message to. */
    private transient ByteArrayOutputStream arrayOutputStream;

    private static final Object NONE_MARKER = new Object();

    /** Tells if we should run schema validation before writing out records. */
    public enum ValidateMode {
        VALIDATE_BEFORE_WRITE,
        NONE
    }

    public JsonRegistrySerializationSchema(
            SchemaRegistryConfig registryConfig,
            RowType rowType,
            ValidateMode validateBeforeWrite) {
        this.rowType = rowType;
        this.schemaRegistryConfig = registryConfig;
        this.validateBeforeWrite = validateBeforeWrite;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        final SchemaRegistryClient schemaRegistryClient =
                schemaRegistryConfig.createClient(context.getJobID().orElse(null));
        this.schemaCoder =
                new SchemaRegistryCoder(schemaRegistryConfig.getSchemaId(), schemaRegistryClient);
        final ParsedSchema schema =
                schemaRegistryClient.getSchemaById(schemaRegistryConfig.getSchemaId());
        this.jsonSchema = (Schema) schema.rawSchema();
        this.runtimeConverter = RowDataToJsonConverters.createConverter(rowType, jsonSchema);
        this.objectMapper =
                new ObjectMapper()
                        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                        .registerModule(new JsonOrgModule())
                        .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
        this.arrayOutputStream = new ByteArrayOutputStream();
    }

    @Override
    public byte[] serialize(RowData row) {
        if (node == null) {
            node = objectMapper.createObjectNode();
        }

        try {
            final JsonNode converted = runtimeConverter.convert(objectMapper, node, row);

            if (validateBeforeWrite == ValidateMode.VALIDATE_BEFORE_WRITE) {
                validate(converted);
            }
            arrayOutputStream.reset();
            schemaCoder.writeSchema(arrayOutputStream);
            objectMapper.writeValue(arrayOutputStream, converted);
            return arrayOutputStream.toByteArray();
        } catch (ValidationException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Invalid JSON: %s\nJSON pointer:%s",
                            e.getErrorMessage(), e.getPointerToViolation()));
        } catch (Throwable t) {
            throw new FlinkRuntimeException(String.format("Could not serialize row '%s'.", row), t);
        }
    }

    private void validate(Object value) throws JsonProcessingException, ValidationException {
        Object primitiveValue = NONE_MARKER;
        if (isPrimitive(value)) {
            primitiveValue = value;
        } else if (value instanceof BinaryNode) {
            primitiveValue = ((BinaryNode) value).asText();
        } else if (value instanceof BooleanNode) {
            primitiveValue = ((BooleanNode) value).asBoolean();
        } else if (value instanceof NullNode) {
            primitiveValue = null;
        } else if (value instanceof NumericNode) {
            primitiveValue = ((NumericNode) value).numberValue();
        } else if (value instanceof TextNode) {
            primitiveValue = ((TextNode) value).asText();
        }
        if (primitiveValue != NONE_MARKER) {
            jsonSchema.validate(primitiveValue);
        } else {
            Object jsonObject;
            if (value instanceof ArrayNode) {
                jsonObject = objectMapper.treeToValue(((ArrayNode) value), JSONArray.class);
            } else if (value instanceof JsonNode) {
                jsonObject = objectMapper.treeToValue(((JsonNode) value), JSONObject.class);
            } else if (value.getClass().isArray()) {
                jsonObject = objectMapper.convertValue(value, JSONArray.class);
            } else {
                jsonObject = objectMapper.convertValue(value, JSONObject.class);
            }
            jsonSchema.validate(jsonObject);
        }
    }

    private static boolean isPrimitive(Object value) {
        return value == null
                || value instanceof Boolean
                || value instanceof Number
                || value instanceof String;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JsonRegistrySerializationSchema that = (JsonRegistrySerializationSchema) o;
        return Objects.equals(rowType, that.rowType)
                && Objects.equals(schemaRegistryConfig, that.schemaRegistryConfig)
                && validateBeforeWrite == that.validateBeforeWrite;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, schemaRegistryConfig, validateBeforeWrite);
    }
}
