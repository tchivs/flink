/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.registry;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for Schema Registry Avro format. */
@Confluent
public class AvroRegistryFormatOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("schema-registry.url")
                    .withDescription(
                            "The URL of the Confluent Schema Registry to fetch/register schemas.");

    public static final ConfigOption<Integer> SCHEMA_ID =
            ConfigOptions.key("schema-id")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The id of the corresponding Avro schema in Schema Registry."
                                    + " This is important for serialization schema. It ensures the"
                                    + " serialization schema writes records in that particular version."
                                    + " That way one can control e.g. records namespaces.");

    public static final ConfigOption<Integer> SCHEMA_CACHE_SIZE =
            ConfigOptions.key("schema-cache-size")
                    .intType()
                    .defaultValue(20)
                    .withDescription("Maximum number of cached schemas.");

    public static final ConfigOption<String> LOGICAL_CLUSTER_ID =
            ConfigOptions.key("logical-cluster-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The kafka logical cluster id in CC");

    public static final ConfigOption<CredentialsSource> CREDENTIALS_SOURCE =
            ConfigOptions.key("credentials-source")
                    .enumType(CredentialsSource.class)
                    .defaultValue(CredentialsSource.DPAT)
                    .withDescription("Where to get the credentials from");

    public static final ConfigOption<String> BASIC_AUTH_USER_INFO =
            ConfigOptions.key("basic-auth.user-info")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Basic auth user info for schema registry");

    /** Where to get the credentials from. */
    public enum CredentialsSource {
        KEYS,
        DPAT
    }

    private AvroRegistryFormatOptions() {}
}
