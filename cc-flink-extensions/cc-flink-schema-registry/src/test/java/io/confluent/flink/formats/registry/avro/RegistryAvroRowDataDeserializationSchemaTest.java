/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.avro;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.UserCodeClassLoader;

import io.confluent.flink.formats.converters.avro.AvroToFlinkSchemaConverter;
import io.confluent.flink.formats.registry.utils.TestSchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvroRegistrySerializationSchema}. */
@ExtendWith(TestLoggerExtension.class)
class RegistryAvroRowDataDeserializationSchemaTest {

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
    void testDeserializingMapAndFixed() throws Exception {
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

        final GenericRowData expected = new GenericRowData(1);
        final Map<DecimalData, Float> customMap = new HashMap<>();
        final DecimalData decimalData = DecimalData.fromBigDecimal(new BigDecimal("12.34"), 4, 2);
        customMap.put(decimalData, 123f);
        expected.setField(0, new GenericMapData(customMap));

        final RowData deserializedRow =
                deserialize(
                        schema,
                        new GenericRecordBuilder(schema)
                                .set(
                                        "custom_map",
                                        Collections.singletonList(
                                                new GenericRecordBuilder(mapEntrySchema)
                                                        .set(
                                                                "key",
                                                                new Fixed(
                                                                        fixedSchema,
                                                                        decimalData
                                                                                .toUnscaledBytes()))
                                                        .set("value", 123f)
                                                        .build()))
                                .build());

        assertThat(deserializedRow).isEqualTo(expected);
    }

    @Test
    void testDeserialisingIntTypes() throws Exception {
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

        final Object actual =
                deserialize(
                        schema,
                        new GenericRecordBuilder(schema)
                                .set("int8", 1)
                                .set("int16", 1)
                                .set("int", 1)
                                .set("time", 1)
                                .set("date", 1)
                                .build());

        assertThat(actual).isEqualTo(rowData);
    }

    @Test
    void testDeserialisingLongTypes() throws Exception {
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

        final Object actual =
                deserialize(
                        schema,
                        new GenericRecordBuilder(schema)
                                .set("timestampMillis", 1L)
                                .set("timestampMicros", 1000L)
                                .set("localTimestampMillis", 1L)
                                .set("localTimestampMicros", 1000L)
                                .set("long", 1L)
                                .build());

        assertThat(actual).isEqualTo(rowData);
    }

    @Test
    void testDeserializingNonRecordType() throws Exception {
        final Schema schema = SchemaBuilder.builder().stringType();

        final GenericRowData rowData = new GenericRowData(1);
        rowData.setField(0, StringData.fromString("ABCD"));

        final Object actual = deserialize(schema, "ABCD");

        assertThat(actual).isEqualTo(rowData);
    }

    private static RowData deserialize(Schema schema, Object record) throws Exception {
        final int schemaId = client.register(SUBJECT, new AvroSchema(schema));
        final LogicalType flinkSchema = AvroToFlinkSchemaConverter.toFlinkSchema(schema);
        final RowType rowType;
        if (flinkSchema.is(LogicalTypeRoot.ROW)) {
            rowType = (RowType) flinkSchema;
        } else {
            rowType =
                    new RowType(false, Collections.singletonList(new RowField("f0", flinkSchema)));
        }
        final TestSchemaRegistryConfig schemaRegistryConfig =
                new TestSchemaRegistryConfig(schemaId, client);
        final DeserializationSchema<RowData> deserializationSchema =
                new AvroRegistryDeserializationSchema(
                        schemaRegistryConfig, rowType, InternalTypeInfo.of(flinkSchema));
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        writeSchemaId(stream, schemaId);
        new GenericDatumWriter<>(schema)
                .write(record, EncoderFactory.get().directBinaryEncoder(stream, null));

        deserializationSchema.open(new MockInitializationContext());
        return deserializationSchema.deserialize(stream.toByteArray());
    }

    private static void writeSchemaId(ByteArrayOutputStream stream, int schemaId) {
        stream.write(0 /* CONFLUENT MAGIC BYTE */);
        stream.write(schemaId >>> 24);
        stream.write(schemaId >>> 16);
        stream.write(schemaId >>> 8);
        stream.write(schemaId);
    }

    private static class MockInitializationContext
            implements DeserializationSchema.InitializationContext,
                    SerializationSchema.InitializationContext {

        @Override
        public MetricGroup getMetricGroup() {
            return new UnregisteredMetricsGroup();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(getClass().getClassLoader());
        }
    }
}
