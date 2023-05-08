/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.avro.registry.converters.AvroToFlinkSchemaConverter;
import io.confluent.flink.formats.avro.registry.utils.MockInitializationContext;
import io.confluent.flink.formats.avro.registry.utils.TestSchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvroRegistrySerializationSchema}. */
@ExtendWith(TestLoggerExtension.class)
class RegistryAvroRowDataSerializationSchemaTest {

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
    void testSerialisingMapAndFixedDecimal() throws Exception {
        final Schema fixedSchema = SchemaBuilder.fixed("test").size(2);
        final Schema mapEntrySchema =
                SchemaBuilder.record("MapEntry")
                        .namespace("io.confluent.connect.avro")
                        .fields()
                        .name("key")
                        .type(LogicalTypes.decimal(4, 2).addToSchema(fixedSchema))
                        .noDefault()
                        .name("value")
                        .type()
                        .floatBuilder()
                        .endFloat()
                        .noDefault()
                        .endRecord();
        final Schema schema =
                SchemaBuilder.builder()
                        .record("top")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("custom_map")
                        .type()
                        .array()
                        .items(mapEntrySchema)
                        .noDefault()
                        .endRecord();

        final GenericRowData rowData = new GenericRowData(1);
        final Map<DecimalData, Float> customMap = new HashMap<>();
        final DecimalData decimalData = DecimalData.fromBigDecimal(new BigDecimal("12.34"), 4, 2);
        customMap.put(decimalData, 123f);
        rowData.setField(0, new GenericMapData(customMap));

        final Object actual = serialize(schema, rowData);

        assertThat(actual)
                .isEqualTo(
                        new GenericRecordBuilder(schema)
                                .set(
                                        "custom_map",
                                        Collections.singletonList(
                                                new GenericRecordBuilder(mapEntrySchema)
                                                        .set(
                                                                "key",
                                                                new GenericData.Fixed(
                                                                        fixedSchema,
                                                                        decimalData
                                                                                .toUnscaledBytes()))
                                                        .set("value", 123f)
                                                        .build()))
                                .build());
    }

    @Test
    void testSerialisingIntTypes() throws Exception {
        final Schema int8Type = SchemaBuilder.builder().intType();
        final Schema int16Type = SchemaBuilder.builder().intType();
        int8Type.addProp("connect.type", "int8");
        int16Type.addProp("connect.type", "int16");
        final Schema schema =
                SchemaBuilder.builder()
                        .record("top")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("int8")
                        .type(int8Type)
                        .noDefault()
                        .name("int16")
                        .type(int16Type)
                        .noDefault()
                        .name("int")
                        .type()
                        .intType()
                        .noDefault()
                        .name("time")
                        .type(
                                LogicalTypes.timeMillis()
                                        .addToSchema(SchemaBuilder.builder().intType()))
                        .noDefault()
                        .name("date")
                        .type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()))
                        .noDefault()
                        .endRecord();

        final GenericRowData rowData = new GenericRowData(5);
        rowData.setField(0, (byte) 1);
        rowData.setField(1, (short) 1);
        rowData.setField(2, 1);
        rowData.setField(3, 1);
        rowData.setField(4, 1);

        final Object actual = serialize(schema, rowData);

        assertThat(actual)
                .isEqualTo(
                        new GenericRecordBuilder(schema)
                                .set("int8", 1)
                                .set("int16", 1)
                                .set("int", 1)
                                .set("time", 1)
                                .set("date", 1)
                                .build());
    }

    @Test
    void testSerialisingLongTypes() throws Exception {
        final Schema timestampMillis = SchemaBuilder.builder().longType();
        final Schema timestampMicros = SchemaBuilder.builder().longType();
        final Schema localTimestampMillis = SchemaBuilder.builder().longType();
        final Schema localTimestampMicros = SchemaBuilder.builder().longType();
        final Schema schema =
                SchemaBuilder.builder()
                        .record("top")
                        .namespace("io.confluent.test")
                        .fields()
                        .name("timestampMillis")
                        .type(LogicalTypes.timestampMillis().addToSchema(timestampMillis))
                        .noDefault()
                        .name("timestampMicros")
                        .type(LogicalTypes.timestampMicros().addToSchema(timestampMicros))
                        .noDefault()
                        .name("localTimestampMillis")
                        .type(LogicalTypes.localTimestampMillis().addToSchema(localTimestampMillis))
                        .noDefault()
                        .name("localTimestampMicros")
                        .type(LogicalTypes.localTimestampMicros().addToSchema(localTimestampMicros))
                        .noDefault()
                        .name("long")
                        .type()
                        .longType()
                        .noDefault()
                        .endRecord();

        final GenericRowData rowData = new GenericRowData(5);
        rowData.setField(0, TimestampData.fromEpochMillis(1));
        rowData.setField(1, TimestampData.fromEpochMillis(1));
        rowData.setField(2, TimestampData.fromEpochMillis(1));
        rowData.setField(3, TimestampData.fromEpochMillis(1));
        rowData.setField(4, 1L);

        final Object actual = serialize(schema, rowData);

        assertThat(actual)
                .isEqualTo(
                        new GenericRecordBuilder(schema)
                                .set("timestampMillis", 1L)
                                .set("timestampMicros", 1000L)
                                .set("localTimestampMillis", 1L)
                                .set("localTimestampMicros", 1000L)
                                .set("long", 1L)
                                .build());
    }

    private static Object serialize(Schema schema, GenericRowData rowData) throws Exception {
        final int schemaId = client.register(SUBJECT, new AvroSchema(schema));

        final LogicalType flinkSchema = AvroToFlinkSchemaConverter.toFlinkSchema(schema);
        final SerializationSchema<RowData> serializationSchema =
                new AvroRegistrySerializationSchema(
                        new TestSchemaRegistryConfig(schemaId, client), (RowType) flinkSchema);
        serializationSchema.open(new MockInitializationContext());
        final byte[] avroSerialised = serializationSchema.serialize(rowData);
        final ByteArrayInputStream stream = new ByteArrayInputStream(avroSerialised);
        assertThat(stream.skip(1 /* magic byte */ + 4 /* schema id*/)).isEqualTo(5);
        return new GenericDatumReader<>(schema)
                .read(null, DecoderFactory.get().binaryDecoder(stream, null));
    }
}
