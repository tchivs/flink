/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.flink.formats.converters.json.JsonToFlinkSchemaConverter;
import io.confluent.flink.formats.registry.json.JsonRegistrySerializationSchema.ValidateMode;
import io.confluent.flink.formats.registry.utils.MockInitializationContext;
import io.confluent.flink.formats.registry.utils.TestSchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonRegistrySerializationSchemaTest {

    private static final String SUBJECT = "test-subject";

    private static SchemaRegistryClient client;

    @BeforeAll
    static void beforeClass() {
        client = new MockSchemaRegistryClient();
    }

    @AfterEach
    void after() throws IOException, RestClientException {
        client.deleteSubject(SUBJECT);
    }

    @Test
    void testSerialisingWithVerification() throws Exception {
        NumberSchema firstSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema secondSchema =
                NumberSchema.builder()
                        .minimum(5)
                        .maximum(10)
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
                        .build();
        ObjectSchema schema =
                ObjectSchema.builder()
                        .addPropertySchema("a", firstSchema)
                        .addPropertySchema("b", secondSchema)
                        .build();

        final GenericRowData input = new GenericRowData(2);
        input.setField(0, (byte) 12);
        input.setField(1, (short) 12);

        assertThatThrownBy(() -> serialize(schema, input, ValidateMode.VALIDATE_BEFORE_WRITE))
                .hasMessage("Invalid JSON: 12 is not less or equal to 10\n" + "JSON pointer:#/b");
    }

    @Test
    void testSerialisingWithoutVerification() throws Exception {
        NumberSchema firstSchema =
                NumberSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int8"))
                        .build();
        NumberSchema secondSchema =
                NumberSchema.builder()
                        .minimum(5)
                        .maximum(10)
                        .unprocessedProperties(Collections.singletonMap("connect.type", "int16"))
                        .build();
        ObjectSchema schema =
                ObjectSchema.builder()
                        .addPropertySchema("a", firstSchema)
                        .addPropertySchema("b", secondSchema)
                        .build();

        final GenericRowData input = new GenericRowData(2);
        input.setField(0, (byte) 12);
        input.setField(1, (short) 12);

        assertThat(serialize(schema, input, ValidateMode.NONE)).isEqualTo("{\"a\":12,\"b\":12}");
    }

    private static Object serialize(
            Schema schema, GenericRowData rowData, ValidateMode validateMode) throws Exception {
        final int schemaId = client.register(SUBJECT, new JsonSchema(schema));

        final LogicalType flinkSchema = JsonToFlinkSchemaConverter.toFlinkSchema(schema);
        final SerializationSchema<RowData> serializationSchema =
                new JsonRegistrySerializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client),
                        (RowType) flinkSchema,
                        validateMode);
        serializationSchema.open(new MockInitializationContext());
        final byte[] jsonSerialised = serializationSchema.serialize(rowData);
        final ByteArrayInputStream stream = new ByteArrayInputStream(jsonSerialised);
        assertThat(stream.skip(1 /* magic byte */ + 4 /* schema id*/)).isEqualTo(5);
        int n = stream.available();
        byte[] bytes = new byte[n];
        stream.read(bytes, 0, n);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
