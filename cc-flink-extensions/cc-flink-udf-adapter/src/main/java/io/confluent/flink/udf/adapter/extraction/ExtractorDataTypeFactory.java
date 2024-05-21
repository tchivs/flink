/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.extraction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.extraction.DataTypeExtractor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

/** A {@link DataTypeFactory} used during metadata extraction. */
public class ExtractorDataTypeFactory implements DataTypeFactory {
    private final ClassLoader classLoader;

    public ExtractorDataTypeFactory(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public DataType createDataType(AbstractDataType<?> abstractDataType) {
        if (abstractDataType instanceof DataType) {
            return (DataType) abstractDataType;
        } else if (abstractDataType instanceof UnresolvedDataType) {
            return ((UnresolvedDataType) abstractDataType).toDataType(this);
        }
        throw new IllegalStateException();
    }

    @Override
    public DataType createDataType(String typeString) {
        return TypeConversions.fromLogicalToDataType(
                LogicalTypeParser.parse(typeString, classLoader));
    }

    @Override
    public DataType createDataType(UnresolvedIdentifier identifier) {
        throw new IllegalStateException("No type found.");
    }

    @Override
    public <T> DataType createDataType(Class<T> clazz) {
        return DataTypeExtractor.extractFromType(this, clazz);
    }

    @Override
    public <T> DataType createDataType(TypeInformation<T> typeInfo) {
        return TypeInfoDataTypeConverter.toDataType(this, typeInfo);
    }

    @Override
    public <T> DataType createRawDataType(Class<T> clazz) {
        throw new IllegalStateException("Raw type not supported");
    }

    @Override
    public <T> DataType createRawDataType(TypeInformation<T> typeInfo) {
        throw new IllegalStateException("Raw type not supported");
    }

    @Override
    public LogicalType createLogicalType(String typeString) {
        return LogicalTypeParser.parse(typeString, classLoader);
    }

    @Override
    public LogicalType createLogicalType(UnresolvedIdentifier identifier) {
        throw new IllegalStateException("No type found.");
    }
}
