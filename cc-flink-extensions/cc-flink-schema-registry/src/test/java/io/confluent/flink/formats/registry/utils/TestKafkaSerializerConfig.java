/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.formats.registry.utils;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;

import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION;

/**
 * Kafka serializer properties useful for serializing messages with schemas containing references.
 * https://docs.confluent.io/cloud/current/sr/fundamentals/serdes-develop/serdes-protobuf.html
 */
public class TestKafkaSerializerConfig {

    private static final Map<String, String> DEFAULT_PROPS =
            ImmutableMap.of(
                    SCHEMA_REGISTRY_URL_CONFIG,
                    "fake-url",

                    // disable auto-registration of the event type, so that it does not override
                    // the union as the latest schema in the subject.
                    AUTO_REGISTER_SCHEMAS,
                    "false",

                    // look up the latest schema version in the subject (which will be the
                    // union) and use that for serialization. Otherwise, the serializer will
                    // look for the event type in the subject and fail to find it.
                    USE_LATEST_VERSION,
                    "true");

    public static Map<String, String> getAvroProps() {
        return DEFAULT_PROPS;
    }

    public static Map<String, String> getProtobufProps() {
        return ImmutableMap.<String, String>builder()
                .putAll(DEFAULT_PROPS)
                // Relaxing the compatibility requirement by setting to false is useful for using
                // schema references.
                // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#handling-differences-between-preregistered-and-client-derived-schemas
                .put(KafkaProtobufSerializerConfig.LATEST_COMPATIBILITY_STRICT, "false")
                // {@link MockSchemaRegistryClient} throws NPE while looking up latest version for a
                // subject's schema from its cache without this
                .put(KafkaProtobufSerializerConfig.REFERENCE_LOOKUP_ONLY_CONFIG, "true")
                .build();
    }
}
