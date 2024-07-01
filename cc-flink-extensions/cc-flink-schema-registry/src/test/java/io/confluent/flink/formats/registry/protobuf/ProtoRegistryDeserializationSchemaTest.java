/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.formats.registry.protobuf;

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.flink.formats.converters.protobuf.ProtoToFlinkSchemaConverter;
import io.confluent.flink.formats.registry.utils.MockInitializationContext;
import io.confluent.flink.formats.registry.utils.TestKafkaSerializerConfig;
import io.confluent.flink.formats.registry.utils.TestSchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ProtoRegistryDeserializationSchema}. */
@ExtendWith(TestLoggerExtension.class)
class ProtoRegistryDeserializationSchemaTest {

    private static final Map<String, String> KAFKA_SERIALIZER_CONFIG =
            TestKafkaSerializerConfig.getProtobufProps();

    // --------------------------------------------------------------------------------------------
    // SCHEMAS TO REGISTER BEFORE EVERY TEST
    // --------------------------------------------------------------------------------------------
    private static final ProtobufSchema PURCHASE_SCHEMA =
            new ProtobufSchema(
                    "syntax = \"proto3\";"
                            + "package io.confluent.developer.proto;"
                            + "message Purchase {"
                            + "  string item = 1;"
                            + "  double amount = 2;"
                            + "  string customer_id = 3;"
                            + "}");

    private static final ProtobufSchema PAGEVIEW_SCHEMA =
            new ProtobufSchema(
                    "syntax = \"proto3\";"
                            + "package io.confluent.developer.proto;"
                            + "message Pageview {"
                            + "  string url = 1;"
                            + "  bool is_special = 2;"
                            + "  string customer_id = 3;"
                            + "}");

    // --------------------------------------------------------------------------------------------
    // REFERENCES FOR REGISTERED SCHEMAS
    // Reference name should be the subject name with default subject naming strategy
    // `TopicNameStrategy` which uses the topic name to determine the subject to be used for schema
    // lookups, and helps to enforce subject-topic constraints.
    // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-protobuf.html#multiple-event-types-in-the-same-topic
    // --------------------------------------------------------------------------------------------
    private static final SchemaReference PURCHASE_REFERENCE =
            new SchemaReference("Purchase-subject", "Purchase-subject", 1);
    private static final SchemaReference PAGEVIEW_REFERENCE =
            new SchemaReference("Pageview-subject", "Pageview-subject", 1);

    private KafkaProtobufSerializer protobufSerializer;
    private SchemaRegistryClient client;

    @BeforeEach
    void init() throws IOException, RestClientException {
        client = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        protobufSerializer = new KafkaProtobufSerializer(client, KAFKA_SERIALIZER_CONFIG);

        // register purchase and pageView schemas
        client.register("Purchase-subject", PURCHASE_SCHEMA);
        client.register("Pageview-subject", PAGEVIEW_SCHEMA);
    }

    @Test
    void testDeserializeMultipleEventTypes() throws Exception {
        // Given:  schema for customer events with field action which can be either
        // purchase or pageView
        // +-------------------+
        // | customer-event    |
        // |-------------------|
        // | action oneof      |
        // |   +--------------+|
        // |   | purchase     ||
        // |OR |--------------+|
        // |   | pageview     ||
        // |   +--------------+|
        // |                   |
        // | id string         |
        // +-------------------+
        ProtobufSchema customerEventProto =
                (ProtobufSchema)
                        client.parseSchema(
                                        "PROTOBUF",
                                        "syntax = \"proto3\";"
                                                + "package io.confluent.developer.proto;"
                                                + "import \"Purchase-subject\";"
                                                + "import \"Pageview-subject\";"
                                                + "message CustomerEvent {"
                                                + "    oneof action {"
                                                + "        Purchase purchase = 1;"
                                                + "        Pageview pageview = 2;"
                                                + "    }"
                                                + "    string id = 3;"
                                                + "}",
                                        List.of(PURCHASE_REFERENCE, PAGEVIEW_REFERENCE))
                                .get();

        // Given: registered under subject customer-event-value
        int id = client.register("customer-event-value", customerEventProto);

        // Given: purchase message is serialized to the customer-event topic
        // (implicitly using customer-event-value schema)
        Object customerEvent =
                ProtobufSchemaUtils.toObject(
                        "{\n"
                                + "\"id\":\"event-1\",\n"
                                + "\"purchase\":{\n"
                                + "     \"item\":\"apple\",\n"
                                + "     \"amount\":123.45,\n"
                                + "     \"customerId\":\"u-1234567890\"\n"
                                + "     }\n"
                                + "}",
                        customerEventProto);

        byte[] serializedCustomerEvent =
                protobufSerializer.serialize("customer-event", customerEvent);

        // When: the message is deserialized with the deserialization schema
        RowData rowData =
                initializeDeserializationSchema(customerEventProto, id)
                        .deserialize(serializedCustomerEvent);

        // Then: the message is deserialized to a row
        assertThat(rowData)
                .satisfies(
                        row -> {
                            // The row has two fields
                            assertThat(row.getArity()).isEqualTo(2);

                            // The id field is a string
                            assertThat(row.getString(1).toString()).isEqualTo("event-1");

                            // The action field is a row with two fields (purchase and pageview)
                            RowData action = row.getRow(0, 1);
                            assertThat(action.getArity()).isEqualTo(2);

                            // Exactly one of purchase or pageview is not null
                            assertThat(action.isNullAt(0) ^ action.isNullAt(1)).isTrue();

                            // The purchase field is a row with three fields (item, amount,
                            // customerId)
                            RowData purchase = action.getRow(0, 1);
                            verifyPurchase(purchase);
                        });
    }

    @Test
    void testDeserializeMultipleMessages() throws Exception {
        // Given: a protobuf schema with two messages purchase and pageview
        ProtobufSchema pageviewPurchaseProto =
                new ProtobufSchema(
                        "syntax = \"proto3\";\n"
                                + "package mypackage;\n"
                                + "\n"
                                + "message Purchase {"
                                + "  string item = 1;"
                                + "  double amount = 2;"
                                + "  string customer_id = 3;"
                                + "}\n"
                                + "message Pageview {"
                                + "  string url = 1;"
                                + "  bool is_special = 2;"
                                + "  string customer_id = 3;"
                                + "}");

        // Given: registered under subject pageview-purchase-value
        int id = client.register("pageview-purchase-value", pageviewPurchaseProto);

        // Given: purchase message is serialized to the pageview-event topic
        // (implicitly using pageview-event-value schema)
        Descriptor purchaseDescriptor = PURCHASE_SCHEMA.toDescriptor();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(purchaseDescriptor);
        builder.setField(purchaseDescriptor.findFieldByName("item"), "apple");
        builder.setField(purchaseDescriptor.findFieldByName("amount"), 123.45);
        builder.setField(purchaseDescriptor.findFieldByName("customer_id"), "u-1234567890");
        Object purchaseEvent = builder.build();

        byte[] serializedPurchaseEvent =
                protobufSerializer.serialize("pageview-purchase", purchaseEvent);

        // When: the message is deserialized with the deserialization schema
        RowData rowData =
                initializeDeserializationSchema(pageviewPurchaseProto, id)
                        .deserialize(serializedPurchaseEvent);

        // Then: the message is deserialized to a row
        verifyPurchase(rowData);
    }

    @Test
    void testDeserializeMultipleMessagesWithNestedSchema() throws Exception {
        // Given: Action has 2 nested messages Purchase and Pageview
        ProtobufSchema actionProto =
                new ProtobufSchema(
                        "syntax = \"proto3\";\n"
                                + "package mypackage;\n"
                                + "\n"
                                + "message Action {\n"
                                + "  Purchase purchase = 1;\n"
                                + "  Pageview pageview = 2;\n"
                                + "}\n"
                                + "message Purchase {\n"
                                + "  string item = 1;\n"
                                + "  double amount = 2;\n"
                                + "  string customer_id = 3;\n"
                                + "}\n"
                                + "message Pageview {\n"
                                + "  string url = 1;\n"
                                + "  bool is_special = 2;\n"
                                + "  string customer_id = 3;\n"
                                + "}\n");

        // Given: registered under subject pageview-purchase-value
        int id = client.register("action-value", actionProto);
        FileDescriptor fileDescriptor = actionProto.toDescriptor().getFile();

        // serialize different messages
        Object purchaseEvent =
                ProtobufSchemaUtils.toObject(
                        "{\"item\": \"apple\", \"amount\": 123.45, \"customer_id\": \"u-1234567890\"}",
                        new ProtobufSchema(fileDescriptor.findMessageTypeByName("Purchase")));

        Object actionEvent =
                ProtobufSchemaUtils.toObject(
                        "{\n"
                                + "  \"purchase\": {\n"
                                + "    \"item\": \"apple\",\n"
                                + "    \"amount\": 123.45,\n"
                                + "    \"customer_id\": \"u-1234567890\"\n"
                                + "  },\n"
                                + "  \"pageview\": {\n"
                                + "    \"url\": \"https://example.com\",\n"
                                + "    \"is_special\": true,\n"
                                + "    \"customer_id\": \"u-1234567890\"\n"
                                + "  }\n"
                                + "}",
                        new ProtobufSchema(fileDescriptor.findMessageTypeByName("Action")));

        // serialize purchase as top level input
        byte[] serializedPurchase = protobufSerializer.serialize("action", purchaseEvent);
        // serialize action as top level input
        byte[] serializedAction = protobufSerializer.serialize("action", actionEvent);

        ProtoRegistryDeserializationSchema deserializationSchema =
                initializeDeserializationSchema(actionProto, id);

        // When: the message is deserialized with the deserialization schema
        RowData purchaseAsTopLevelInput = deserializationSchema.deserialize(serializedPurchase);

        // Then: the message is deserialized to a row
        verifyPurchase(purchaseAsTopLevelInput);

        // When: the message is deserialized with the deserialization schema
        RowData actionAsTopLevelInput = deserializationSchema.deserialize(serializedAction);

        // Then: the message is deserialized to a row with two fields purchase and pageview
        assertThat(actionAsTopLevelInput)
                .satisfies(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            RowData purchase = row.getRow(0, 1);
                            verifyPurchase(purchase);
                            RowData pageview = row.getRow(1, 1);
                            verifyPageview(pageview);
                        });
    }

    @Test
    void testDeserializeWithNotNull() throws Exception {
        // Given: Action has 2 nested messages Purchase and Pageview
        ProtobufSchema actionProto =
                new ProtobufSchema(
                        "syntax = \"proto3\";\n"
                                + "package io.confluent.developer.proto;\n"
                                + "\n"
                                + "message Action {\n"
                                + "  Purchase purchase = 1;\n"
                                + "  Pageview pageview = 2 [(confluent.field_meta) = {\n"
                                + "    params: [\n"
                                + "      {\n"
                                + "        key: \"flink.version\",\n"
                                + "        value: \"1\"\n"
                                + "      },\n"
                                + "      {\n"
                                + "        key: \"flink.notNull\",\n"
                                + "        value: \"true\"\n"
                                + "      }\n"
                                + "    ]\n"
                                + "  }];\n"
                                + "}\n"
                                + "message Purchase {\n"
                                + "  string item = 1;\n"
                                + "  double amount = 2;\n"
                                + "  string customer_id = 3;\n"
                                + "}\n"
                                + "message Pageview {\n"
                                + "  string url = 1;\n"
                                + "  bool is_special = 2;\n"
                                + "  string customer_id = 3;\n"
                                + "}\n");

        // Given: registered under subject pageview-purchase-value
        int id = client.register("action-value", actionProto);
        FileDescriptor fileDescriptor = actionProto.toDescriptor().getFile();

        // serialize action message without purchase
        Object actionWithoutPurchase =
                ProtobufSchemaUtils.toObject(
                        "{\n"
                                + "  \"pageview\": {\n"
                                + "    \"url\": \"https://example.com\",\n"
                                + "    \"is_special\": true,\n"
                                + "    \"customer_id\": \"u-1234567890\"\n"
                                + "  }\n"
                                + "}",
                        new ProtobufSchema(fileDescriptor.findMessageTypeByName("Action")));

        // serialize purchase as top level input
        byte[] serializedActionWithoutPurchase =
                protobufSerializer.serialize("action", actionWithoutPurchase);
        ProtoRegistryDeserializationSchema deserializationSchema =
                initializeDeserializationSchema(actionProto, id);

        // When: the message is deserialized with the deserialization schema
        RowData actionWithoutPurchaseRow =
                deserializationSchema.deserialize(serializedActionWithoutPurchase);

        // Then: the message is deserialized to a row
        assertThat(actionWithoutPurchaseRow)
                .satisfies(
                        row -> {
                            assertThat(row.getArity()).isEqualTo(2);
                            assertThat(row.isNullAt(0)).isTrue();
                            assertThat(row.isNullAt(1)).isFalse();
                            RowData pageview = row.getRow(1, 1);
                            verifyPageview(pageview);
                        });
    }

    @Test
    void testDeserializeMessageSerializedWithNestedTypeFails() throws Exception {
        // Given: a schema with a nested type Purchase
        ProtobufSchema actionProto =
                new ProtobufSchema(
                        "syntax = \"proto3\";\n"
                                + "package mypackage;\n"
                                + "\n"
                                + "message Action {\n"
                                + "  Purchase purrrrrrr = 1;\n"
                                + "  message Purchase {\n"
                                + "    string item = 1;\n"
                                + "    double amount = 2;\n"
                                + "    string customer_id = 3;\n"
                                + "  }\n"
                                + "}\n");

        // Given: registered under subject action-value
        int id = client.register("action-value", actionProto);

        // serialize Purchase message with Purchase schema
        Object purchase =
                ProtobufSchemaUtils.toObject(
                        "{\n"
                                + "  \"item\": \"apple\",\n"
                                + "  \"amount\": 123.45,\n"
                                + "  \"customer_id\": \"u-1234567890\"\n"
                                + "}",
                        new ProtobufSchema(actionProto.toDescriptor().getNestedTypes().get(0)));

        // serialize purchase as top level input
        byte[] serializedPurchase = protobufSerializer.serialize("action", purchase);

        // open a deserialization schema with Action schema
        ProtoRegistryDeserializationSchema deserializationSchema =
                initializeDeserializationSchema(actionProto, id);
        // When: the message is deserialized with the deserialization schema
        assertThatThrownBy(() -> deserializationSchema.deserialize(serializedPurchase))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to deserialize Protobuf message.")
                .cause()
                .isInstanceOf(TableRuntimeException.class)
                .hasMessage(
                        "Payload could not be deserialized. The 'protobuf-registry' format only supports deserialization of messages that have been defined as top-level messages in the schema. Nested messages as top-level messages is not supported.");
    }

    // --------------------------------------------------------------------------------------------
    // HELPERS
    // --------------------------------------------------------------------------------------------
    /* Create and open a deserialization schema for a schema and its ID. */
    private ProtoRegistryDeserializationSchema initializeDeserializationSchema(
            ProtobufSchema protobufSchema, int schemaId) throws Exception {
        LogicalType flinkType =
                ProtoToFlinkSchemaConverter.toFlinkSchema(protobufSchema.toDescriptor().getFile());

        ProtoRegistryDeserializationSchema deserializationSchema =
                new ProtoRegistryDeserializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client),
                        (RowType) flinkType,
                        InternalTypeInfo.of(flinkType));

        deserializationSchema.open(new MockInitializationContext());
        return deserializationSchema;
    }

    /* Verify that a purchase row is valid. */
    private void verifyPurchase(RowData row) {
        assertThat(row.getString(0).toString()).isEqualTo("apple");
        assertThat(row.getDouble(1)).isEqualTo(123.45);
        assertThat(row.getString(2).toString()).isEqualTo("u-1234567890");
    }

    private void verifyPageview(RowData row) {
        assertThat(row.getString(0).toString()).isEqualTo("https://example.com");
        assertThat(row.getBoolean(1)).isTrue();
        assertThat(row.getString(2).toString()).isEqualTo("u-1234567890");
    }
}
