/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

/**
 * Common constants used for converting JSON schema. Those are Confluent Schema Registry specific.
 */
public class CommonConstants {

    static final String CONNECT_TYPE_PROP = "connect.type";

    static final String CONNECT_TYPE_INT8 = "int8";
    static final String CONNECT_TYPE_INT16 = "int16";
    static final String CONNECT_TYPE_INT32 = "int32";
    static final String CONNECT_TYPE_INT64 = "int64";
    static final String CONNECT_TYPE_FLOAT32 = "float32";
    static final String CONNECT_TYPE_FLOAT64 = "float64";
    static final String CONNECT_TYPE_BYTES = "bytes";
    static final String CONNECT_TYPE_MAP = "map";
    static final String KEY_FIELD = "key";
    static final String VALUE_FIELD = "value";
    static final String CONNECT_INDEX_PROP = "connect.index";
    static final String CONNECT_TYPE_TIME = "org.apache.kafka.connect.data.Time";
    static final String CONNECT_TYPE_DATE = "org.apache.kafka.connect.data.Date";
    static final String CONNECT_TYPE_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";

    static final String CONNECT_TYPE_DECIMAL = "org.apache.kafka.connect.data.Decimal";
    static final String CONNECT_TYPE_DECIMAL_SCALE = "scale";
    static final String CONNECT_TYPE_DECIMAL_PRECISION = "connect.decimal.precision";
    static final String GENERALIZED_TYPE_UNION_PREFIX = "connect_union_field_";
}
