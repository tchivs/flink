/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

/**
 * Common constants used for converting JSON schema. Those are Confluent Schema Registry specific.
 */
public class CommonConstants {

    public static final String CONNECT_TYPE_PROP = "connect.type";
    public static final String CONNECT_TYPE_INT8 = "int8";
    public static final String CONNECT_TYPE_INT16 = "int16";
    public static final String CONNECT_TYPE_INT32 = "int32";
    public static final String CONNECT_TYPE_INT64 = "int64";
    public static final String CONNECT_TYPE_FLOAT32 = "float32";
    public static final String CONNECT_TYPE_FLOAT64 = "float64";
    public static final String CONNECT_TYPE_BYTES = "bytes";
    public static final String CONNECT_TYPE_MAP = "map";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String CONNECT_INDEX_PROP = "connect.index";
    public static final String CONNECT_TYPE_TIME = "org.apache.kafka.connect.data.Time";
    public static final String CONNECT_TYPE_DATE = "org.apache.kafka.connect.data.Date";
    public static final String CONNECT_TYPE_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";
    public static final String CONNECT_TYPE_DECIMAL = "org.apache.kafka.connect.data.Decimal";
    public static final String CONNECT_PARAMETERS = "connect.parameters";
    public static final String CONNECT_TYPE_DECIMAL_SCALE = "scale";
    public static final String CONNECT_TYPE_DECIMAL_PRECISION = "connect.decimal.precision";
    public static final String GENERALIZED_TYPE_UNION_PREFIX = "connect_union_field_";

    // ====================================================
    // Flink specific properties. We need those to cover Flink types which are not supported
    // by neither JSON nor have extensions in Kafka Connect. Seperate from the above in case
    // Kafka Connect adds support for them in the future.
    // ====================================================
    public static final String FLINK_TYPE_PROP = "flink.type";
    public static final String FLINK_TYPE_TIMESTAMP = "timestamp";
    public static final String FLINK_TYPE_MULTISET = "multiset";
    public static final String FLINK_PARAMETERS = "flink.parameters";
    public static final String FLINK_PRECISION = "precision";
    static final String FLINK_MIN_LENGTH = "flink.minLength";
    static final String FLINK_MAX_LENGTH = "flink.maxLength";
}
