/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry;

import org.apache.flink.annotation.Confluent;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static java.lang.String.format;

/** Reads and Writes schema using Confluent Schema Registry protocol. */
@Confluent
public class SchemaRegistryCoder {

    private final SchemaRegistryClient schemaRegistryClient;
    private static final int CONFLUENT_MAGIC_BYTE = 0;
    private final int schemaId;

    public SchemaRegistryCoder(int schemaId, SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.schemaId = schemaId;
    }

    public ParsedSchema readSchema(InputStream in) throws IOException {
        DataInputStream dataInputStream = new DataInputStream(in);

        if (dataInputStream.readByte() != 0) {
            throw new IOException("Unknown data format. Magic number does not match");
        } else {
            int schemaId = dataInputStream.readInt();

            try {
                return schemaRegistryClient.getSchemaById(schemaId);
                // we assume this is avro schema
            } catch (RestClientException e) {
                throw new IOException(
                        format("Could not find schema with id %s in registry", schemaId), e);
            }
        }
    }

    public void writeSchema(OutputStream out) throws IOException {
        // we do not check the schema, but write the id that we were initialised with
        out.write(CONFLUENT_MAGIC_BYTE);
        writeInt(out, schemaId);
    }

    private static void writeInt(OutputStream out, int registeredId) throws IOException {
        out.write(registeredId >>> 24);
        out.write(registeredId >>> 16);
        out.write(registeredId >>> 8);
        out.write(registeredId);
    }
}
