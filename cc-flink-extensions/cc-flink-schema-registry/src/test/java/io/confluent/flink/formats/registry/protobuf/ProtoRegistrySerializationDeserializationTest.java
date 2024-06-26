/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.protobuf;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.TestLoggerExtension;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.flink.formats.converters.protobuf.FlinkToProtoSchemaConverter;
import io.confluent.flink.formats.converters.protobuf.ProtoToFlinkSchemaConverter;
import io.confluent.flink.formats.registry.utils.MockInitializationContext;
import io.confluent.flink.formats.registry.utils.TestKafkaSerializerConfig;
import io.confluent.flink.formats.registry.utils.TestSchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke tests for checking {@link ProtoRegistrySerializationSchema} and {@link
 * ProtoRegistryDeserializationSchema}.
 *
 * <p>For more thorough tests on converting different types see {@link RowDataToProtoConvertersTest}
 * and/or {@link ProtoToRowDataConvertersTest}.
 */
@ExtendWith(TestLoggerExtension.class)
class ProtoRegistrySerializationDeserializationTest {
    private static final Map<String, String> KAFKA_SERIALIZER_CONFIG =
            TestKafkaSerializerConfig.getProtobufProps();

    private static final String SUBJECT = "test-subject";

    private SchemaRegistryClient client;
    private KafkaProtobufSerializer serializer;

    @BeforeEach
    void setUp() {
        client = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
        serializer = new KafkaProtobufSerializer(client, KAFKA_SERIALIZER_CONFIG);
    }

    @Test
    void testSerDe() throws Exception {
        final RowType rowType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField("booleanNotNull", new BooleanType(false)),
                                new RowField("tinyIntNotNull", new TinyIntType(false)),
                                new RowField("smallIntNotNull", new SmallIntType(false)),
                                new RowField("intNotNull", new IntType(false)),
                                new RowField("bigintNotNull", new BigIntType(false)),
                                new RowField("doubleNotNull", new DoubleType(false)),
                                new RowField("floatNotNull", new FloatType(false)),
                                new RowField("date", new DateType(true)),
                                new RowField("decimal", new DecimalType(true, 5, 1)),
                                new RowField("timestamp", new LocalZonedTimestampType(true, 9)),
                                new RowField("time", new TimeType(true, 3)),
                                new RowField(
                                        "string", new VarCharType(true, VarCharType.MAX_LENGTH)),
                                new RowField(
                                        "bytes",
                                        new VarBinaryType(true, VarBinaryType.MAX_LENGTH))));

        final int timestampSeconds = 960000000;
        final int timestampNanos = 34567890;
        final GenericRowData row = new GenericRowData(13);
        row.setField(0, true);
        row.setField(1, (byte) 42);
        row.setField(2, (short) 42);
        row.setField(3, 42);
        row.setField(4, 42L);
        row.setField(5, 42D);
        row.setField(6, 42F);
        row.setField(7, DateTimeUtils.toInternal(LocalDate.of(2023, 9, 4)));
        row.setField(8, DecimalData.fromBigDecimal(BigDecimal.valueOf(12345L, 1), 5, 1));
        row.setField(
                9,
                TimestampData.fromEpochMillis(
                        timestampSeconds * 1000L + timestampNanos / 1000_000,
                        timestampNanos % 1000_000));
        row.setField(10, DateTimeUtils.toInternal(LocalTime.of(16, 45, 1, 999_000_000)));
        row.setField(11, StringData.fromString("Random string"));
        row.setField(12, new byte[] {1, 2, 3});

        final Descriptor descriptor =
                FlinkToProtoSchemaConverter.fromFlinkSchema(
                        rowType, "Row", "io.confluent.generated");
        final int schemaId = client.register(SUBJECT, new ProtobufSchema(descriptor));
        final byte[] serialized = serialize(schemaId, row, rowType);
        final RowData deserialized = deserialize(serialized, schemaId, rowType);
        assertThat(deserialized).isEqualTo(row);
    }

    @Test
    void testRoundTrip() throws Exception {
        final ProtobufSchema stockTradeSchema =
                new ProtobufSchema(
                        "syntax = \"proto3\";\n"
                                + "package ksql;\n"
                                + "\n"
                                + "message StockTrade {\n"
                                + "  string side = 1;\n"
                                + "  int32 quantity = 2;\n"
                                + "  string symbol = 3;\n"
                                + "  int32 price = 4;\n"
                                + "  string account = 5;\n"
                                + "  string userid = 6;\n"
                                + "}");
        final int schemaId = client.register(SUBJECT + "-value", stockTradeSchema, 0, 100002);
        final LogicalType flinkSchema =
                ProtoToFlinkSchemaConverter.toFlinkSchema(
                        stockTradeSchema.toDescriptor().getFile());

        // Create a StockTrade message
        Object stockTrade =
                ProtobufSchemaUtils.toObject(
                        "{\n"
                                + "\"side\":\"SELL\",\n"
                                + "\"quantity\":776,\n"
                                + "\"symbol\":\"ZBZX\",\n"
                                + "\"price\":107,\n"
                                + "\"account\":\"XYZ789\",\n"
                                + "\"userid\":\"User_5\"\n"
                                + "}",
                        stockTradeSchema);

        // Serialize the StockTrade message
        byte[] serializedData = serializer.serialize(SUBJECT, stockTrade);

        // Deserialize the message
        final DeserializationSchema<RowData> deserializationSchema =
                new ProtoRegistryDeserializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client),
                        (RowType) flinkSchema,
                        InternalTypeInfo.of(flinkSchema));
        deserializationSchema.open(new MockInitializationContext());
        final RowData deserialized = deserializationSchema.deserialize(serializedData);

        // Expected RowData
        final GenericRowData expected = new GenericRowData(6);
        expected.setField(0, StringData.fromString("SELL"));
        expected.setField(1, 776);
        expected.setField(2, StringData.fromString("ZBZX"));
        expected.setField(3, 107);
        expected.setField(4, StringData.fromString("XYZ789"));
        expected.setField(5, StringData.fromString("User_5"));

        assertThat(deserialized).isEqualTo(expected);
    }

    private byte[] serialize(int schemaId, GenericRowData rowData, RowType flinkSchema)
            throws Exception {

        final SerializationSchema<RowData> serializationSchema =
                new ProtoRegistrySerializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client), flinkSchema);
        serializationSchema.open(new MockInitializationContext());
        return serializationSchema.serialize(rowData);
    }

    private RowData deserialize(byte[] data, int schemaId, RowType flinkSchema) throws Exception {

        final DeserializationSchema<RowData> serializationSchema =
                new ProtoRegistryDeserializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client),
                        flinkSchema,
                        InternalTypeInfo.of(flinkSchema));
        serializationSchema.open(new MockInitializationContext());
        return serializationSchema.deserialize(data);
    }
}
