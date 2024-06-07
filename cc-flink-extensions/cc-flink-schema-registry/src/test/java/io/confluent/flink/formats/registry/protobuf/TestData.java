/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.protobuf;

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.flink.formats.converters.protobuf.CommonConstants;
import io.confluent.flink.formats.converters.protobuf.CommonMappings;

import java.util.Arrays;
import java.util.Collections;

/**
 * Data that can be commonly used both in {@link ProtoToRowDataConvertersTest} and {@link
 * RowDataToProtoConvertersTest}.
 */
public class TestData {

    /** Contains RowData and its Protobuf equivalent. */
    public static class DataMapping {

        final DynamicMessage proto;
        final RowData flink;

        public DataMapping(DynamicMessage proto, RowData flink) {
            this.proto = proto;
            this.flink = flink;
        }
    }

    /** @see CommonMappings#NULLABLE_ARRAYS_CASE */
    public static DataMapping createDataForNullableArraysCase() {
        final Descriptor schema = CommonMappings.NULLABLE_ARRAYS_CASE.getProtoSchema();

        // repeated elementNullableArrayElement elementNullable = 2 [(confluent.field_meta) = {
        final FieldDescriptor elementNullableField = schema.findFieldByName("elementNullable");

        // arrayAndElementNullableRepeatedWrapper arrayAndElementNullable = 3
        // [(confluent.field_meta) = {
        final FieldDescriptor arrayAndElementNullableWrapperField =
                schema.findFieldByName("arrayAndElementNullable");

        //   message arrayAndElementNullableRepeatedWrapper {
        //    repeated valueArrayElement value = 1 [(confluent.field_meta) = {
        final FieldDescriptor arrayAndElementNullableField =
                arrayAndElementNullableWrapperField.getMessageType().getFields().get(0);

        //     message valueArrayElement {
        //      optional int64 element = 1;
        final FieldDescriptor arrayAndElementNullableElementField =
                arrayAndElementNullableField
                        .getMessageType()
                        .findFieldByName(CommonConstants.FLINK_WRAPPER_FIELD_NAME);

        final GenericRowData row = new GenericRowData(3);
        row.setField(0, null);
        row.setField(1, new GenericArrayData(new Long[] {null, 1L}));
        row.setField(2, new GenericArrayData(new Long[] {null, 3L}));

        final DynamicMessage message =
                DynamicMessage.newBuilder(schema)
                        // field 0 not set, representing null
                        .setField(
                                elementNullableField,
                                Arrays.asList(
                                        // null representation, field not set
                                        DynamicMessage.newBuilder(
                                                        elementNullableField.getMessageType())
                                                .build(),
                                        DynamicMessage.newBuilder(
                                                        elementNullableField.getMessageType())
                                                .setField(
                                                        elementNullableField
                                                                .getMessageType()
                                                                .findFieldByName(
                                                                        CommonConstants
                                                                                .FLINK_WRAPPER_FIELD_NAME),
                                                        1L)
                                                .build()))
                        .setField(
                                arrayAndElementNullableWrapperField,
                                DynamicMessage.newBuilder(
                                                arrayAndElementNullableWrapperField
                                                        .getMessageType())
                                        .setField(
                                                arrayAndElementNullableField,
                                                Arrays.asList(
                                                        // null representation, field not set
                                                        DynamicMessage.newBuilder(
                                                                        arrayAndElementNullableField
                                                                                .getMessageType())
                                                                .build(),
                                                        DynamicMessage.newBuilder(
                                                                        arrayAndElementNullableField
                                                                                .getMessageType())
                                                                .setField(
                                                                        arrayAndElementNullableElementField,
                                                                        3L)
                                                                .build()))
                                        .build())
                        .build();

        return new DataMapping(message, row);
    }

    /** @see CommonMappings#NULLABLE_COLLECTIONS_CASE */
    public static DataMapping createDataForNullableCollectionsCase() {
        final Descriptor schema = CommonMappings.NULLABLE_COLLECTIONS_CASE.getProtoSchema();

        // repeated arrayOfMapsArrayElement arrayOfMaps = 2 [(confluent.field_meta) = {
        final FieldDescriptor arrayOfMapsField = schema.findFieldByName("arrayOfMaps");

        // nullableArrayOfNullableMapsRepeatedWrapper nullableArrayOfNullableMaps = 3
        // [(confluent.field_meta) = {
        final FieldDescriptor nullableArrayOfNullableMapsWrapperField =
                schema.findFieldByName("nullableArrayOfNullableMaps");

        // message nullableArrayOfNullableMapsRepeatedWrapper {
        //    repeated valueArrayElement value = 1 [(confluent.field_meta) = {
        final FieldDescriptor nullableArrayOfNullableMapsField =
                nullableArrayOfNullableMapsWrapperField.getMessageType().getFields().get(0);

        // repeated MapofnullablearraysEntry mapOfNullableArrays = 4;
        final FieldDescriptor mapOfNullableArraysField =
                schema.findFieldByName("mapOfNullableArrays");

        //   message mapOfNullableArraysRepeatedWrapper {
        //    repeated ValueEntry value = 1;
        final FieldDescriptor mapValueArrayWrapper =
                mapOfNullableArraysField.getMessageType().findFieldByName("value");

        //     message valueRepeatedWrapper {
        //      repeated int64 value = 1;
        final FieldDescriptor mapValueArray =
                mapValueArrayWrapper.getMessageType().getFields().get(0);

        final DynamicMessage proto =
                DynamicMessage.newBuilder(schema)
                        // field 0 not set, representing null
                        .setField(
                                arrayOfMapsField,
                                Collections.singletonList(
                                        // null representation, field not set
                                        getMapForArrayOfMaps(arrayOfMapsField)))
                        .setField(
                                nullableArrayOfNullableMapsWrapperField,
                                DynamicMessage.newBuilder(
                                                nullableArrayOfNullableMapsWrapperField
                                                        .getMessageType())
                                        .setField(
                                                nullableArrayOfNullableMapsField,
                                                Collections.singletonList(
                                                        DynamicMessage.newBuilder(
                                                                        nullableArrayOfNullableMapsField
                                                                                .getMessageType())
                                                                .build()))
                                        .build())
                        .setField(
                                mapOfNullableArraysField,
                                Collections.singletonList(
                                        DynamicMessage.newBuilder(
                                                        mapOfNullableArraysField.getMessageType())
                                                .setField(
                                                        mapOfNullableArraysField
                                                                .getMessageType()
                                                                .findFieldByName("key"),
                                                        1L)
                                                .setField(
                                                        mapValueArrayWrapper,
                                                        DynamicMessage.newBuilder(
                                                                        mapValueArrayWrapper
                                                                                .getMessageType())
                                                                .setField(
                                                                        mapValueArray,
                                                                        Collections.singletonList(
                                                                                1L))
                                                                .build())
                                                .build()))
                        .build();

        final GenericRowData row = new GenericRowData(4);
        row.setField(0, null);
        row.setField(
                1,
                new GenericArrayData(
                        new MapData[] {new GenericMapData(Collections.singletonMap(1L, 2L))}));
        row.setField(2, new GenericArrayData(new MapData[] {null}));
        row.setField(
                3,
                new GenericMapData(
                        Collections.singletonMap(1L, new GenericArrayData(new Long[] {1L}))));

        return new DataMapping(proto, row);
    }

    private static DynamicMessage getMapForArrayOfMaps(FieldDescriptor arrayOfMapsField) {
        final FieldDescriptor arrayOfMapsMapType =
                arrayOfMapsField
                        .getMessageType()
                        .findFieldByName(CommonConstants.FLINK_WRAPPER_FIELD_NAME);
        return DynamicMessage.newBuilder(arrayOfMapsField.getMessageType())
                .setField(
                        arrayOfMapsMapType,
                        Collections.singletonList(
                                DynamicMessage.newBuilder(arrayOfMapsMapType.getMessageType())
                                        .setField(
                                                arrayOfMapsMapType
                                                        .getMessageType()
                                                        .findFieldByName("key"),
                                                1L)
                                        .setField(
                                                arrayOfMapsMapType
                                                        .getMessageType()
                                                        .findFieldByName("value"),
                                                2L)
                                        .build()))
                .build();
    }
}
