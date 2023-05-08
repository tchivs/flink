/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry;

import org.apache.flink.util.TestLoggerExtension;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SchemaRegistryCoder}. */
@ExtendWith(TestLoggerExtension.class)
class SchemaRegistryCoderTest {

    @Test
    void testSpecificRecordWithConfluentSchemaRegistry() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();

        Schema schema =
                SchemaBuilder.record("testRecord").fields().optionalString("testField").endRecord();
        int schemaId = client.register("testTopic", schema);

        SchemaRegistryCoder registryCoder = new SchemaRegistryCoder(schemaId, client);
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteOutStream);
        dataOutputStream.writeByte(0);
        dataOutputStream.writeInt(schemaId);
        dataOutputStream.flush();

        ByteArrayInputStream byteInStream = new ByteArrayInputStream(byteOutStream.toByteArray());
        Schema readSchema = registryCoder.readSchema(byteInStream);

        assertThat(readSchema).isEqualTo(schema);
        assertThat(byteInStream).isEmpty();
    }

    @Test
    void testMagicByteVerification() throws Exception {
        MockSchemaRegistryClient client = new MockSchemaRegistryClient();
        int schemaId = client.register("testTopic", Schema.create(Schema.Type.BOOLEAN));

        SchemaRegistryCoder coder = new SchemaRegistryCoder(schemaId, client);
        ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteOutStream);
        dataOutputStream.writeByte(5);
        dataOutputStream.writeInt(schemaId);
        dataOutputStream.flush();

        try (ByteArrayInputStream byteInStream =
                new ByteArrayInputStream(byteOutStream.toByteArray())) {
            assertThatThrownBy(() -> coder.readSchema(byteInStream))
                    .isInstanceOf(IOException.class);
        }
    }
}
