/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.avro;

/**
 * Common constants used for converting AVRO schema. Those are Confluent Schema Registry specific.
 */
class CommonConstants {

    static final String CONNECT_TYPE_PROP = "connect.type";
    static final String KEY_FIELD = "key";
    static final String VALUE_FIELD = "value";
    static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
    static final String AVRO_LOGICAL_DECIMAL = "decimal";
    static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
    static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
    static final String AVRO_LOGICAL_DATE = "date";
    static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
    static final String AVRO_LOGICAL_TIME_MICROS = "time-micros";
    static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
    static final String AVRO_LOGICAL_TIMESTAMP_MICROS = "timestamp-micros";
    static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
    static final String AVRO_LOGICAL_LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";

    // ====================================================
    // Flink specific properties. We need those to cover Flink types which are not supported
    // by neither AVRO nor have extensions in Kafka Connect. Seperate from the above in case
    // Kafka Connect adds support for them in the future.
    // ====================================================
    static final String FLINK_PRECISION = "flink.precision";

    private CommonConstants() {}
}
