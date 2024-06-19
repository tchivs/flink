/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLoggerExtension;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.flink.formats.converters.json.JsonToFlinkSchemaConverter;
import io.confluent.flink.formats.registry.utils.MockInitializationContext;
import io.confluent.flink.formats.registry.utils.TestKafkaSerializerConfig;
import io.confluent.flink.formats.registry.utils.TestSchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.confluent.kafka.schemaregistry.json.JsonSchemaUtils.envelope;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonRegistryDeserializationSchema}. */
@ExtendWith(TestLoggerExtension.class)
class JsonRegistryDeserializationSchemaTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Map<String, String> KAFKA_SERIALIZER_CONFIG =
            TestKafkaSerializerConfig.getJsonProps();

    // --------------------------------------------------------------------------------------------
    // SCHEMAS TO REGISTER BEFORE EVERY TEST
    // --------------------------------------------------------------------------------------------
    private static final String PURCHASE_SCHEMA =
            "{ \"$id\": \"http://cflt.io/Purchase.json\","
                    + " \"type\": \"object\","
                    + " \"properties\": {"
                    + " \"id\": { \"type\": \"string\" },"
                    + " \"amount\": { \"type\": \"integer\" },"
                    + " \"currency\": { \"type\": \"string\" }"
                    + " },"
                    + " \"required\": [ \"id\", \"amount\", \"currency\" ]"
                    + " }";

    private static final String PAGEVIEW_SCHEMA =
            "{ \"$id\": \"http://cflt.io/PageView.json\","
                    + " \"title\": \"PageView\","
                    + " \"type\": \"object\","
                    + " \"properties\": {"
                    + " \"id\": { \"type\": \"string\" },"
                    + " \"page\": { \"type\": \"string\" },"
                    + " \"user\": { \"type\": \"string\" },"
                    + " \"time\": { \"type\": \"integer\" }"
                    + " },"
                    + " \"required\": [ \"id\", \"page\", \"user\", \"time\" ]"
                    + " }"
                    + "}";

    // --------------------------------------------------------------------------------------------
    // REFERENCES FOR REGISTERED SCHEMAS
    // --------------------------------------------------------------------------------------------
    private static final SchemaReference PURCHASE_REFERENCE =
            new SchemaReference("http://cflt.io/Purchase.json", "purchase", 1);
    private static final SchemaReference PAGEVIEW_REFERENCE =
            new SchemaReference("http://cflt.io/PageView.json", "pageview", 1);

    private SchemaRegistryClient client;
    private KafkaJsonSchemaSerializer serializer;

    @BeforeEach
    void setUp() throws IOException, RestClientException {
        client = new MockSchemaRegistryClient(List.of(new JsonSchemaProvider()));

        // Register purchase and pageview schemas
        client.register("purchase", new JsonSchema(PURCHASE_SCHEMA));
        client.register("pageview", new JsonSchema(PAGEVIEW_SCHEMA));

        // Serializer for creating Kafka messages from JSON
        serializer = new KafkaJsonSchemaSerializer(client, KAFKA_SERIALIZER_CONFIG);
    }

    @Test
    void testDeserializeWithReferences() throws Exception {
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
        JsonSchema transactionSchemaForRegistry =
                ((JsonSchema)
                        client.parseSchema(
                                        "JSON",
                                        "{ \"$id\": \"http://cflt.io/Transaction.json\","
                                                + " \"title\": \"Transaction\","
                                                + " \"type\": \"object\","
                                                + " \"properties\": {"
                                                + " \"paymentGateway\": { \"type\": \"string\" },"
                                                + " \"purchase\": { \"$ref\": \"http://cflt.io/Purchase.json\" }"
                                                + " },"
                                                + " \"required\": [ \"paymentGateway\", \"purchase\" ] }",
                                        List.of(PURCHASE_REFERENCE))
                                .get());

        // Given: registered under subject transaction-value
        int id = client.register("transaction-value", transactionSchemaForRegistry);

        // Given: purchase message is serialized to the customer-event topic
        // (implicitly using transaction-value schema)
        byte[] transactionMessage =
                serializer.serialize(
                        "transaction",
                        envelope(
                                transactionSchemaForRegistry,
                                MAPPER.readTree(
                                        "{\n"
                                                + "  \"paymentGateway\": \"PayPal\",\n"
                                                + "  \"purchase\": {\n"
                                                + "    \"id\": \"1\",\n"
                                                + "    \"amount\": 999,\n"
                                                + "    \"currency\": \"USD\"\n"
                                                + "  }\n"
                                                + "}")));

        RowData flinkRowFromTransaction =
                deserialize(transactionSchemaForRegistry, id, transactionMessage);

        assertThat(flinkRowFromTransaction)
                .satisfies(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            assertThat(row.getString(0).toString()).isEqualTo("PayPal");
                            RowData nestedRow = row.getRow(1, 3);
                            verifyPurchaseRow(nestedRow);
                        });
    }

    @Test
    void testDeserializeMultipleEventTypes() throws Exception {
        // Given: registered schema for customer events which can be either purchase or pageView
        // +-------------------+
        // | customer-value    |
        // |-------------------|
        // | oneOf             |
        // |   +--------------+|
        // |   | Purchase     ||
        // |OR |--------------+|
        // |   | PageView     ||
        // |   +--------------+|
        // +-------------------+
        JsonSchema schemaForRegistry =
                ((JsonSchema)
                        client.parseSchema(
                                        "JSON",
                                        "{"
                                                + "\"oneOf\": ["
                                                + "  { \"$ref\": \"http://cflt.io/Purchase.json\" },"
                                                + "  { \"$ref\": \"http://cflt.io/PageView.json\" }"
                                                + "]"
                                                + "}",
                                        List.of(PURCHASE_REFERENCE, PAGEVIEW_REFERENCE))
                                .get());

        // Given: oneOf schema registered under subject customer-events-value
        int id = client.register("customer-events-value", schemaForRegistry);

        // Given: purchase message is serialized to the customer-event topic
        // (implicitly using transaction-value schema)
        byte[] oneOfWithPurchaseMessage =
                serializer.serialize(
                        "customer-events",
                        envelope(
                                schemaForRegistry,
                                MAPPER.readTree(
                                        "{\n"
                                                + "  \"id\": \"1\",\n"
                                                + "  \"amount\": 999,\n"
                                                + "  \"currency\": \"USD\"\n"
                                                + "}")));

        RowData flinkRowFromPurchase = deserialize(schemaForRegistry, id, oneOfWithPurchaseMessage);

        assertThat(flinkRowFromPurchase)
                .satisfies(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            RowData nestedRow = row.getRow(0, 3);
                            verifyPurchaseRow(nestedRow);
                            assertThat(row.getString(1)).isNull();
                        });
    }

    @Test
    void testDeserializeSchemaWithNestedReferences() throws Exception {
        // Given: registered order schema with purchase and pageView references as properties
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
        JsonSchema orderSchemaForRegistry =
                ((JsonSchema)
                        client.parseSchema(
                                        "JSON",
                                        "{"
                                                + "\"type\": \"object\","
                                                + "\"properties\": {"
                                                + "  \"purchase\": { \"$ref\": \"http://cflt.io/Purchase.json\" },"
                                                + "  \"pageView\": { \"$ref\": \"http://cflt.io/PageView.json\" }"
                                                + "}"
                                                + "}",
                                        List.of(PURCHASE_REFERENCE, PAGEVIEW_REFERENCE))
                                .get());
        client.register("order", orderSchemaForRegistry);

        // Given: registered nested schema with order reference as a property
        // +-------------------+
        // | Nested            |
        // |-------------------|
        // | name: string      |
        // | order:            |
        // |   +--------------+|
        // |   | Order        ||
        // |   +--------------+|
        // +-------------------+
        List<SchemaReference> orderReference =
                List.of(new SchemaReference("http://cflt.io/Order.json", "order", 1));
        JsonSchema nestedSchemaForRegistry =
                ((JsonSchema)
                        client.parseSchema(
                                        "JSON",
                                        "{"
                                                + "\"type\": \"object\","
                                                + "\"properties\": {"
                                                + "  \"name\": { \"type\": \"string\" },"
                                                + "  \"order\": { \"$ref\": \"http://cflt.io/Order.json\" }"
                                                + "}"
                                                + "}",
                                        orderReference)
                                .get());
        int nestedSchemaId = client.register("nested-value", nestedSchemaForRegistry);

        // When: nested JSON Node is deserialized
        // Nested
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
        byte[] messageFromNestedNode =
                serializer.serialize(
                        "nested",
                        envelope(
                                nestedSchemaForRegistry,
                                MAPPER.readTree(
                                        "{"
                                                + "  \"name\": \"John Doe\","
                                                + "  \"order\": {"
                                                + "    \"purchase\": {"
                                                + "      \"id\": \"1\","
                                                + "      \"amount\": 999,"
                                                + "      \"currency\": \"USD\""
                                                + "    },"
                                                + "    \"pageView\": {"
                                                + "      \"id\": \"2\","
                                                + "      \"page\": \"groceries\","
                                                + "      \"user\": \"Jane Smith\","
                                                + "      \"time\": 1234567890"
                                                + "    }"
                                                + "  }"
                                                + "}")));

        RowData flinkRowFromMessage =
                deserialize(nestedSchemaForRegistry, nestedSchemaId, messageFromNestedNode);

        // Then: the deserialized row has fields "name" and "order" with correct values
        assertThat(flinkRowFromMessage)
                .satisfies(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            assertThat(row.getString(0).toString()).isEqualTo("John Doe");
                            RowData orderRow = row.getRow(1, 2);
                            verifyPageViewRow(orderRow.getRow(0, 3));
                            verifyPurchaseRow(orderRow.getRow(1, 4));
                        });
    }

    private RowData deserialize(JsonSchema jsonSchema, int schemaId, byte[] message)
            throws Exception {
        LogicalType transactionFlinkType =
                JsonToFlinkSchemaConverter.toFlinkSchema(jsonSchema.rawSchema());

        JsonRegistryDeserializationSchema deserializationSchema =
                new JsonRegistryDeserializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client),
                        (RowType) transactionFlinkType,
                        InternalTypeInfo.of(transactionFlinkType));

        deserializationSchema.open(new MockInitializationContext());
        return deserializationSchema.deserialize(message);
    }

    private void verifyPurchaseRow(RowData row) {
        assertThat(row.getLong(0)).isEqualTo(999);
        assertThat(row.getString(1).toString()).isEqualTo("USD");
        assertThat(row.getString(2).toString()).isEqualTo("1");
    }

    private void verifyPageViewRow(RowData row) {
        assertThat(row.getString(0).toString()).isEqualTo("2");
        // note: fields are not in the same order as in the schema as JSON keys are unordered
        // fields at indices 1 and 2 are swapped
        assertThat(row.getString(1).toString()).isEqualTo("groceries");
        assertThat(row.getLong(2)).isEqualTo(1234567890);
        assertThat(row.getString(3).toString()).isEqualTo("Jane Smith");
    }
}
