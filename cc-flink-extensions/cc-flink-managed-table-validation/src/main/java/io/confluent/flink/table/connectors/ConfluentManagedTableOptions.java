/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.connector.ChangelogMode;

import io.confluent.flink.table.connectors.ConfluentManagedFormats.PublicAvroRegistryFormat;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Options for Confluent managed tables. */
@Confluent
public class ConfluentManagedTableOptions {

    // --------------------------------------------------------------------------------------------
    // PUBLIC - GLOBAL FOR ALL TABLES VIA SET COMMAND
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Duration> SQL_TABLES_SCAN_IDLE_TIMEOUT =
            ConfigOptions.key("sql.tables.scan.idle-timeout")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            "If a table's partition does not receive any elements within the given interval, it will be "
                                    + "marked as temporarily idle. This allows downstream tasks to advance their "
                                    + "watermarks without the need to wait for watermarks from all inputs. The "
                                    + "default value is 0 which means that idle detection is disabled.");

    public static final ConfigOption<GlobalScanStartupMode> SQL_TABLES_SCAN_STARTUP_MODE =
            ConfigOptions.key("sql.tables.scan.startup.mode")
                    .enumType(GlobalScanStartupMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Overwrites 'scan.startup.mode' for Confluent-native tables used in newly created queries. "
                                    + "This option is not applied if the table uses a value that differs from the default value.");

    public static final ConfigOption<Long> SQL_TABLES_SCAN_STARTUP_MILLIS =
            ConfigOptions.key("sql.tables.scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Overwrites 'scan.startup.timestamp-millis' for Confluent-native tables used in newly created queries. "
                                    + "This option is not applied if the table has already set a value.");

    public static final ConfigOption<GlobalScanBoundedMode> SQL_TABLES_SCAN_BOUNDED_MODE =
            ConfigOptions.key("sql.tables.scan.bounded.mode")
                    .enumType(GlobalScanBoundedMode.class)
                    .noDefaultValue()
                    .withDescription(
                            "Overwrites 'scan.bounded.mode' for Confluent-native tables used in newly created queries. "
                                    + "This option is not applied if the table uses a value that differs from the default value.");

    public static final ConfigOption<Long> SQL_TABLES_SCAN_BOUNDED_MILLIS =
            ConfigOptions.key("sql.tables.scan.bounded.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Overwrites 'scan.bounded.timestamp-millis' for Confluent-native tables used in newly created queries. "
                                    + "This option is not applied if the table has already set a value.");

    /** Enum for {@link #SQL_TABLES_SCAN_STARTUP_MODE}. */
    public enum GlobalScanStartupMode {
        EARLIEST_OFFSET("earliest-offset"),
        LATEST_OFFSET("latest-offset"),
        GROUP_OFFSETS("group-offsets"),
        TIMESTAMP("timestamp");

        private final String value;

        GlobalScanStartupMode(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /** Enum for {@link #SQL_TABLES_SCAN_BOUNDED_MODE}. */
    public enum GlobalScanBoundedMode {
        LATEST_OFFSET("latest-offset"),
        GROUP_OFFSETS("group-offsets"),
        TIMESTAMP("timestamp"),
        UNBOUNDED("unbounded");

        private final String value;

        GlobalScanBoundedMode(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    // --------------------------------------------------------------------------------------------
    // PUBLIC - PER TABLE
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<ManagedConnector> CONNECTOR =
            ConfigOptions.key("connector")
                    .enumType(ManagedConnector.class)
                    .defaultValue(ManagedConnector.CONFLUENT)
                    .withDescription("Confluent-managed connectors that can be used.");

    // --------------------------------------------------------------------------------------------
    // PUBLIC - RUNTIME SPECIFIC
    // --------------------------------------------------------------------------------------------

    // Note: Use default value only if they should show up after a CREATE TABLE by default.

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
                    .defaultValue(ScanStartupMode.EARLIEST_OFFSET)
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
                    .defaultValue(ScanBoundedMode.UNBOUNDED)
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
                    .defaultValue(PublicAvroRegistryFormat.IDENTIFIER)
                    .withDescription("Message value format.");

    public static final ConfigOption<FieldsInclude> VALUE_FIELDS_INCLUDE =
            ConfigOptions.key("value.fields-include")
                    .enumType(FieldsInclude.class)
                    .noDefaultValue()
                    .withDescription("Whether to include the key fields in the value.");

    public static final Set<ConfigOption<?>> PUBLIC_RUNTIME_OPTIONS = initPublicRuntimeOptions();

    private static Set<ConfigOption<?>> initPublicRuntimeOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CHANGELOG_MODE);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SCAN_BOUNDED_MODE);
        options.add(SCAN_BOUNDED_SPECIFIC_OFFSETS);
        options.add(SCAN_BOUNDED_TIMESTAMP_MILLIS);
        options.add(KEY_FORMAT);
        options.add(KEY_FIELDS_PREFIX);
        options.add(VALUE_FORMAT);
        options.add(VALUE_FIELDS_INCLUDE);
        return Collections.unmodifiableSet(options);
    }

    // --------------------------------------------------------------------------------------------
    // PUBLIC - CREATE TABLE SPECIFIC
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<CleanupPolicy> KAFKA_CLEANUP_POLICY =
            ConfigOptions.key("kafka.cleanup-policy")
                    .enumType(CleanupPolicy.class)
                    .defaultValue(CleanupPolicy.DELETE)
                    .withDescription("Translates to Kafka's log.cleanup.policy.");

    public static final ConfigOption<Integer> KAFKA_PARTITIONS =
            ConfigOptions.key("kafka.partitions")
                    .intType()
                    .defaultValue(6)
                    .withDescription("Translates to Kafka's num.partitions.");

    public static final ConfigOption<Duration> KAFKA_RETENTION_TIME =
            ConfigOptions.key("kafka.retention.time")
                    .durationType()
                    .defaultValue(Duration.ofDays(7))
                    .withDescription("Translates to Kafka's log.retention.ms.");

    public static final ConfigOption<MemorySize> KAFKA_RETENTION_SIZE =
            ConfigOptions.key("kafka.retention.size")
                    .memoryType()
                    .defaultValue(MemorySize.ZERO)
                    .withDescription("Translates to Kafka's log.retention.bytes.");

    public static final ConfigOption<MemorySize> KAFKA_MAX_MESSAGE_SIZE =
            ConfigOptions.key("kafka.max-message-size")
                    .memoryType()
                    .defaultValue(new MemorySize(2097164L))
                    .withDescription("Translates to Kafka's max.message.bytes.");

    public static final Set<ConfigOption<?>> PUBLIC_CREATION_OPTIONS = initPublicCreationOptions();

    private static Set<ConfigOption<?>> initPublicCreationOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KAFKA_CLEANUP_POLICY);
        options.add(KAFKA_PARTITIONS);
        options.add(KAFKA_RETENTION_TIME);
        options.add(KAFKA_RETENTION_SIZE);
        options.add(KAFKA_MAX_MESSAGE_SIZE);
        return Collections.unmodifiableSet(options);
    }

    // --------------------------------------------------------------------------------------------
    // PUBLIC - IMMUTABLE
    // --------------------------------------------------------------------------------------------

    public static final Set<ConfigOption<?>> PUBLIC_IMMUTABLE_OPTIONS =
            initPublicImmutableOptions();

    private static Set<ConfigOption<?>> initPublicImmutableOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(KAFKA_CLEANUP_POLICY);
        options.add(KAFKA_PARTITIONS);
        options.add(KAFKA_RETENTION_TIME);
        options.add(KAFKA_RETENTION_SIZE);
        options.add(KAFKA_MAX_MESSAGE_SIZE);
        return Collections.unmodifiableSet(options);
    }

    // --------------------------------------------------------------------------------------------
    // PRIVATE
    // --------------------------------------------------------------------------------------------

    public static final String PRIVATE_PREFIX = "confluent.";

    // --------------------------------------------------------------------------------------------
    // PRIVATE - RUNTIME SPECIFIC - SET BY METASTORE
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> CONFLUENT_KAFKA_TOPIC =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Topic name.");

    public static final ConfigOption<String> CONFLUENT_KAFKA_BOOTSTRAP_SERVERS =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.bootstrap-servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Forwarded as 'bootstrap.servers' for CC.");

    public static final ConfigOption<String> CONFLUENT_KAFKA_LOGICAL_CLUSTER_ID =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.logical-cluster-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Forwarded as 'confluent.kafka.logical.cluster.id' for CC.");

    public static final ConfigOption<CredentialsSource> CONFLUENT_KAFKA_CREDENTIALS_SOURCE =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.credentials-source")
                    .enumType(CredentialsSource.class)
                    .defaultValue(CredentialsSource.DPAT)
                    .withDescription("Where to get the credentials from.");

    public static final ConfigOption<Integer> CONFLUENT_KAFKA_REPLICATION_FACTOR =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.replication-factor")
                    .intType()
                    .defaultValue(3)
                    .withDescription("Replication factor. Must be a short value.");

    public static final ConfigOption<Map<String, String>> CONFLUENT_KAFKA_PROPERTIES =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Properties for advanced configuration or custom credentials.");

    // --------------------------------------------------------------------------------------------
    // PRIVATE - RUNTIME SPECIFIC - SET BY SQL SERVICE
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> CONFLUENT_KAFKA_CONSUMER_GROUP_ID =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.consumer-group-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Forwarded as 'properties.group.id' for CC.");

    public static final ConfigOption<String> CONFLUENT_KAFKA_CLIENT_ID_PREFIX =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.client-id-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prefix for 'client.id' for CC.");

    public static final ConfigOption<String> CONFLUENT_KAFKA_TRANSACTIONAL_ID_PREFIX =
            ConfigOptions.key(PRIVATE_PREFIX + "kafka.transactional-id-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prefix for 'transactional.id' for CC.");

    public static final ConfigOption<SourceWatermarkVersion> CONFLUENT_SOURCE_WATERMARK_VERSION =
            ConfigOptions.key(PRIVATE_PREFIX + "source-watermark.version")
                    .enumType(SourceWatermarkVersion.class)
                    .noDefaultValue()
                    .withDescription(
                            "Version of the watermark generator if the SOURCE_WATERMARK() strategy "
                                    + "is applied.");

    public static final ConfigOption<Boolean> CONFLUENT_SOURCE_WATERMARK_EMIT_PER_ROW =
            ConfigOptions.key(PRIVATE_PREFIX + "source-watermark.emit-per-row")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether or not to emit a watermark for every incoming row. "
                                    + "Mostly intended for testing purposes.");

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Enum for {@link #CONNECTOR}. */
    public enum ManagedConnector {
        CONFLUENT("confluent");

        private final String value;

        ManagedConnector(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

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

    /** Enum for {@link #CONFLUENT_KAFKA_CREDENTIALS_SOURCE}. */
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

    /** Enum for {@link #KAFKA_CLEANUP_POLICY}. */
    public enum CleanupPolicy {
        DELETE("delete"),
        COMPACT("compact"),
        DELETE_COMPACT("delete-compact");

        private final String value;

        CleanupPolicy(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * Enum for {@link #CONFLUENT_SOURCE_WATERMARK_VERSION}.
     *
     * <p>For implementation details check {@code VersionedWatermarkStrategy}.
     */
    public enum SourceWatermarkVersion implements Serializable {

        /**
         * Fallback strategy to 10s out-of-order in case there is a bug in the default
         * implementation.
         */
        V0,

        /** Initial moving histogram of observed delays from the maximum seen timestamp. */
        V1
    }

    private ConfluentManagedTableOptions() {
        // no instantiation
    }
}
