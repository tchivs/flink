/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/** Options for Confluent managed tables. */
@Confluent
public class ConfluentManagedTableOptions {

    // PUBLIC

    public static final ConfigOption<ManagedChangelogMode> CHANGELOG_MODE =
            ConfigOptions.key("changelog.mode")
                    .enumType(ManagedChangelogMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Changelog mode of the connector. A superset of the format's"
                                    + "changelog mode. The final produced and consumed changelog "
                                    + "mode depends on the combination of format and connector. "
                                    + "This is confusing for users which is why the changelog mode "
                                    + "just shows what the table supports (considering connector "
                                    + "and format). E.g. the format might be insert-only but the "
                                    + "connector supports upsert. Thus, the a superset means upsert.");

    public static final ConfigOption<ScanStartupMode> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .enumType(ScanStartupMode.class)
                    .noDefaultValue()
                    .withDescription("Kafka start-up mode.");

    public static final ConfigOption<List<Map<String, String>>> SCAN_STARTUP_SPECIFIC_OFFSETS =
            ConfigOptions.key("scan.startup.specific-offsets")
                    .mapType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Kafka specific offsets.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Kafka startup timestamp.");

    public static final ConfigOption<ScanBoundedMode> SCAN_BOUNDED_MODE =
            ConfigOptions.key("scan.bounded.mode")
                    .enumType(ScanBoundedMode.class)
                    .noDefaultValue()
                    .withDescription("Kafka bounded mode.");

    public static final ConfigOption<List<Map<String, String>>> SCAN_BOUNDED_SPECIFIC_OFFSETS =
            ConfigOptions.key("scan.bounded.specific-offsets")
                    .mapType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Kafka bounded specific offsets.");

    public static final ConfigOption<Long> SCAN_BOUNDED_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.bounded.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Kafka bounded timestamp.");

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Message key format.");

    public static final ConfigOption<String> KEY_FIELDS_PREFIX =
            ConfigOptions.key("key.fields-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prefix for key fields in the schema.");

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Message value format.");

    public static final ConfigOption<FieldsInclude> VALUE_FIELDS_INCLUDE =
            ConfigOptions.key("value.fields-include")
                    .enumType(FieldsInclude.class)
                    .defaultValue(FieldsInclude.EXCEPT_KEY)
                    .withDescription("Whether to include the key fields in the value.");

    // PRIVATE - SET BY METASTORE

    public static final ConfigOption<String> KAFKA_TOPIC =
            ConfigOptions.key("kafka.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Topic name.");

    public static final ConfigOption<String> KAFKA_BOOTSTRAP_SERVERS =
            ConfigOptions.key("kafka.bootstrap-servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Forwarded as 'bootstrap.servers' for CC.");

    public static final ConfigOption<String> KAFKA_LOGICAL_CLUSTER_ID =
            ConfigOptions.key("kafka.logical-cluster-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Forwarded as 'confluent.kafka.logical.cluster.id' for CC.");

    public static final ConfigOption<CredentialsSource> KAFKA_CREDENTIALS_SOURCE =
            ConfigOptions.key("kafka.credentials-source")
                    .enumType(CredentialsSource.class)
                    .defaultValue(CredentialsSource.DPAT)
                    .withDescription("Where to get the credentials from.");

    public static final ConfigOption<Map<String, String>> KAFKA_PROPERTIES =
            ConfigOptions.key("kafka.properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Properties for advanced configuration or custom credentials.");

    // PRIVATE - SET BY JOB SUBMISSION SERVICE

    public static final ConfigOption<String> KAFKA_CONSUMER_GROUP_ID =
            ConfigOptions.key("kafka.consumer-group-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Forwarded as 'properties.group.id' for CC.");

    public static final ConfigOption<String> KAFKA_TRANSACTIONAL_ID_PREFIX =
            ConfigOptions.key("kafka.transactional-id-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prefix for 'transactional.id' for CC.");

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Enum for {@link #CHANGELOG_MODE}. */
    public enum ManagedChangelogMode implements Serializable {
        APPEND("append"),
        UPSERT("upsert"),
        RETRACT("retract");

        private final String value;

        ManagedChangelogMode(String value) {
            this.value = value;
        }

        public ChangelogMode toChangelogMode() {
            switch (this) {
                case APPEND:
                    return ChangelogMode.insertOnly();
                case UPSERT:
                    return ChangelogMode.upsert();
                case RETRACT:
                    return ChangelogMode.all();
                default:
                    throw new IllegalArgumentException("unsupported managed changelog: " + this);
            }
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /** Enum for {@link #SCAN_STARTUP_MODE}. */
    public enum ScanStartupMode {
        EARLIEST_OFFSET("earliest-offset"),
        LATEST_OFFSET("latest-offset"),
        GROUP_OFFSETS("group-offsets"),
        TIMESTAMP("timestamp"),
        SPECIFIC_OFFSETS("specific-offsets");

        private final String value;

        ScanStartupMode(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /** Enum for {@link #SCAN_STARTUP_MODE}. */
    public enum ScanBoundedMode {
        LATEST_OFFSET("latest-offset"),
        GROUP_OFFSETS("group-offsets"),
        TIMESTAMP("timestamp"),
        SPECIFIC_OFFSETS("specific-offsets"),
        UNBOUNDED("unbounded");

        private final String value;

        ScanBoundedMode(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /** Enum for {@link #VALUE_FIELDS_INCLUDE}. */
    public enum FieldsInclude {
        ALL("all"),
        EXCEPT_KEY("except-key");

        private final String value;

        FieldsInclude(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /** Enum for {@link #KAFKA_CREDENTIALS_SOURCE}. */
    public enum CredentialsSource {
        PROPERTIES("properties"),
        DPAT("dpat");

        private final String value;

        CredentialsSource(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private ConfluentManagedTableOptions() {
        // no instantiation
    }
}
