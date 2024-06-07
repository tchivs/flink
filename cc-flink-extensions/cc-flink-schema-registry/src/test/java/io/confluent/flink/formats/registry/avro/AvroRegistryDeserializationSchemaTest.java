/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.formats.registry.avro;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.formats.converters.avro.AvroToFlinkSchemaConverter;
import io.confluent.flink.formats.registry.utils.MockInitializationContext;
import io.confluent.flink.formats.registry.utils.TestSchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/* Test cases for {@link AvroRegistryDeserializationSchema} */
@ExtendWith(TestLoggerExtension.class)
class AvroRegistryDeserializationSchemaTest {
    /**
     * Kafka serializer properties that are used to serialize messages with multiple event types in
     * the same topic. Follow the guide to avoid false positive tests for multiple event types:
     * https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/serdes-avro.html#multiple-event-types-in-the-same-topic
     */
    private static final Map<String, String> KAFKA_AVRO_SERIALIZER_PROPS =
            ImmutableMap.of(
                    KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake-url",
                    // disable auto-registration of the event type, so that it does not override
                    // the union as the latest schema in the subject.
                    KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "false",
                    // look up the latest schema version in the subject (which will be the
                    // union) and use that for serialization. Otherwise, the serializer will
                    // look for the event type in the subject and fail to find it.
                    KafkaAvroSerializerConfig.USE_LATEST_VERSION, "true");

    // --------------------------------------------------------------------------------------------
    // SCHEMAS TO REGISTER BEFORE EVERY TEST
    // --------------------------------------------------------------------------------------------
    private static final Schema purchaseSchema =
            SchemaBuilder.record("Purchase")
                    .namespace("io.confluent")
                    .fields()
                    .requiredString("id")
                    .requiredInt("amount")
                    .requiredString("currency")
                    .endRecord();

    private static final Schema pageViewSchema =
            SchemaBuilder.record("PageView")
                    .namespace("io.confluent")
                    .fields()
                    .requiredString("id")
                    .requiredString("page")
                    .requiredString("user")
                    .requiredLong("time")
                    .endRecord();

    // --------------------------------------------------------------------------------------------
    // REFERENCES FOR REGISTERED SCHEMAS
    // --------------------------------------------------------------------------------------------
    private static final SchemaReference purchaseRef =
            new SchemaReference("io.confluent.Purchase", "purchase", 1);
    private static final SchemaReference pageViewRef =
            new SchemaReference("io.confluent.PageView", "pageView", 1);

    // --------------------------------------------------------------------------------------------
    // TEST RECORDS FOR REGISTERED SCHEMAS
    // --------------------------------------------------------------------------------------------
    private static final GenericRecord purchaseRecord =
            new GenericRecordBuilder(purchaseSchema)
                    .set("id", "1")
                    .set("amount", 999)
                    .set("currency", "USD")
                    .build();

    private static final GenericRecord pageViewRecord =
            new GenericRecordBuilder(pageViewSchema)
                    .set("id", "2")
                    .set("page", "groceries")
                    .set("user", "Jane Smith")
                    .set("time", 1234567890L)
                    .build();

    // --------------------------------------------------------------------------------------------
    // MOCKS
    // --------------------------------------------------------------------------------------------
    private SchemaRegistryClient client;
    private KafkaAvroSerializer avroSerializer;

    @BeforeEach
    void init() throws IOException, RestClientException {
        client = new MockSchemaRegistryClient();
        // register purchase and pageView schemas
        client.register("purchase", new AvroSchema(purchaseSchema));
        client.register("pageView", new AvroSchema(pageViewSchema));

        // serializer for creating kafka messages from AVRO records
        avroSerializer = new KafkaAvroSerializer(client, KAFKA_AVRO_SERIALIZER_PROPS);
    }

    @Test
    void testDeserializeSchemaWithReference() throws Exception {
        // Given: registered transaction schema
        // +-------------------+
        // | transaction-value |
        // |-------------------|
        // | paymentGateway    | -> "PayPal"
        // | purchase (ref)    | -> +-------------------+
        // |                   |    | Purchase          |
        // |                   |    |-------------------|
        // |                   |    | id                | -> "1"
        // |                   |    | amount            | -> 999
        // |                   |    | currency          | -> "USD"
        // |                   |    +-------------------+
        // +-------------------+
        ParsedSchema transactionSchemaForRegistry =
                client.parseSchema(
                                "AVRO",
                                "{\"type\": \"record\", \"name\": \"Transaction\", \"namespace\": \"io.confluent\", \"fields\": ["
                                        + "{\"name\": \"paymentGateway\", \"type\": \"string\"},"
                                        + "{\"name\": \"purchase\", \"type\": \"io.confluent.Purchase\"}]}",
                                List.of(purchaseRef))
                        .get();
        int id = client.register("transaction-value", transactionSchemaForRegistry);

        // When: transaction message is deserialized
        Schema transactionAvroSchema = ((AvroSchema) transactionSchemaForRegistry).rawSchema();

        byte[] transactionMessage =
                avroSerializer.serialize(
                        "transaction",
                        new GenericRecordBuilder(transactionAvroSchema)
                                .set("paymentGateway", "PayPal")
                                .set("purchase", purchaseRecord)
                                .build());

        RowData flinkRowFromTransaction =
                initializeDeserializationSchema(transactionAvroSchema, id)
                        .deserialize(transactionMessage);

        // Then: the deserialized record should have correct values
        assertThat(flinkRowFromTransaction)
                .satisfies(
                        row -> {
                            assertThat(row.getString(0).toString()).isEqualTo("PayPal");
                            RowData actualPurchaseRow = row.getRow(1, 2);
                            verifyPurchase(actualPurchaseRow);
                        });
    }

    @Test
    void testDeserializeMultipleEventTypes() throws Exception {
        // Given: registered schema for customer events which can be either purchase or pageView
        // +-------------------+
        // | customer-value    |
        // |-------------------|
        // | UNION             |
        // |   +--------------+|
        // |   | Purchase     ||
        // |OR |--------------+|
        // |   | PageView     ||
        // |   +--------------+|
        // +-------------------+
        ParsedSchema schemaForRegistry =
                client.parseSchema(
                                "AVRO",
                                // UNION of purchase and pageView
                                "[\"io.confluent.Purchase\",\"io.confluent.PageView\"]",
                                List.of(purchaseRef, pageViewRef))
                        .get();
        int id = client.register("customer-value", schemaForRegistry);

        // When: purchase message is serialized using the customer (union) schema
        Schema avroSchema = ((AvroSchema) schemaForRegistry).rawSchema();
        byte[] purchaseMessage = avroSerializer.serialize("customer", purchaseRecord);
        RowData flinkRowFromPurchase =
                initializeDeserializationSchema(avroSchema, id).deserialize(purchaseMessage);

        // Then: the deserialized record has non-null field as purchase
        assertThat(flinkRowFromPurchase)
                .satisfies(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            RowData actualPurchaseRowData = row.getRow(0, 1);
                            verifyPurchase(actualPurchaseRowData);
                        });
    }

    @Test
    void testDeserializeSchemaWithNestedReferences() throws Exception {
        // Given: registered order schema with purchase and pageView references as fields
        // +-------------------+
        // | Order             |
        // |-------------------|
        // | purchase:         |
        // |   +--------------+|
        // |   | Purchase     ||
        // |   +--------------+|
        // | pageView:         |
        // |   +--------------+|
        // |   | PageView     ||
        // |   +--------------+|
        // +-------------------+
        ParsedSchema orderSchemaForRegistry =
                client.parseSchema(
                                "AVRO",
                                "{\"type\": \"record\", \"name\": \"Order\", \"namespace\": \"io.confluent\", \"fields\": ["
                                        + "{\"name\": \"purchase\", \"type\": \"io.confluent.Purchase\"},"
                                        + "{\"name\": \"pageView\", \"type\": \"io.confluent.PageView\"}]}",
                                List.of(purchaseRef, pageViewRef))
                        .get();
        client.register("order", orderSchemaForRegistry);

        // Given: registered nested schema with order reference as a field
        // +-------------------+
        // | NestedRecord      |
        // |-------------------|
        // | name: string      |
        // | order:            |
        // |   +--------------+|
        // |   | Order        ||
        // |   +--------------+|
        // +-------------------+
        ParsedSchema nestedSchemaForRegistry =
                client.parseSchema(
                                "AVRO",
                                "{\"type\": \"record\",\"name\": \"NestedRecord\",\"fields\": ["
                                        + "{\"name\": \"name\", \"type\": \"string\"},"
                                        + "{\"name\": \"order\", \"type\": \"io.confluent.Order\"}]}",
                                List.of(new SchemaReference("io.confluent.Order", "order", 1)))
                        .get();
        int nestedSchemaId = client.register("nested-value", nestedSchemaForRegistry);

        // When: a message with nested record is deserialized
        // NestedRecord
        // ├── name: John Doe
        // └── order
        //     ├── purchase
        //     │   ├── id: 1
        //     │   ├── amount: 999
        //     │   └── currency: USD
        //     └── pageView
        //         ├── id: 2
        //         ├── page: groceries
        //         ├── user: Jane Smith
        //         └── time: 1234567890
        Schema nestedAvroSchema = ((AvroSchema) nestedSchemaForRegistry).rawSchema();
        byte[] messageFromNestedRecord =
                avroSerializer.serialize(
                        "nested",
                        new GenericRecordBuilder(nestedAvroSchema)
                                .set("name", "John Doe")
                                .set(
                                        "order",
                                        new GenericRecordBuilder(
                                                        ((AvroSchema) orderSchemaForRegistry)
                                                                .rawSchema())
                                                .set("purchase", purchaseRecord)
                                                .set("pageView", pageViewRecord)
                                                .build())
                                .build());

        RowData flinkRowFromMessage =
                initializeDeserializationSchema(nestedAvroSchema, nestedSchemaId)
                        .deserialize(messageFromNestedRecord);

        // Then: the deserialized row has fields "name" and "order" with correct values
        assertThat(flinkRowFromMessage)
                .satisfies(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            assertThat(row.getString(0).toString()).isEqualTo("John Doe");
                            RowData orderRow = row.getRow(1, 2);
                            verifyPurchase(orderRow.getRow(0, 3));
                            verifyPageView(orderRow.getRow(1, 4));
                        });
    }

    /**
     * Helper method to create and open a deserialization schema for a schema and its ID.
     *
     * @param avroSchema used to determine the Flink RowType after deserialization
     * @param schemaId the ID of the schema in the registry
     * @return the deserialization schema
     * @throws Exception if the deserialization schema cannot be opened
     */
    private AvroRegistryDeserializationSchema initializeDeserializationSchema(
            Schema avroSchema, int schemaId) throws Exception {
        LogicalType transactionFlinkType = AvroToFlinkSchemaConverter.toFlinkSchema(avroSchema);

        AvroRegistryDeserializationSchema deserializationSchema =
                new AvroRegistryDeserializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client),
                        (RowType) transactionFlinkType,
                        InternalTypeInfo.of(transactionFlinkType));

        deserializationSchema.open(new MockInitializationContext());
        return deserializationSchema;
    }

    // --------------------------------------------------------------------------------------------
    // HELPERS for verifying the deserialized Flink row
    // --------------------------------------------------------------------------------------------
    private void verifyPurchase(RowData row) {
        assertThat(row.getString(0).toString()).isEqualTo("1");
        assertThat(row.getInt(1)).isEqualTo(999);
        assertThat(row.getString(2).toString()).isEqualTo("USD");
    }

    private void verifyPageView(RowData row) {
        assertThat(row.getString(0).toString()).isEqualTo("2");
        assertThat(row.getString(1).toString()).isEqualTo("groceries");
        assertThat(row.getString(2).toString()).isEqualTo("Jane Smith");
        assertThat(row.getLong(3)).isEqualTo(1234567890L);
    }
}
