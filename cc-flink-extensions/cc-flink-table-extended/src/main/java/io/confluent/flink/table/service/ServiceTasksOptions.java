/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.PLACEHOLDER_SYMBOL;

/** Supported configuration options for {@link ServiceTasks}. */
@Confluent
public final class ServiceTasksOptions {
    public static final String PRIVATE_PREFIX = "confluent.";
    public static final ConfigOption<String> CLIENT =
            ConfigOptions.key("client." + PLACEHOLDER_SYMBOL)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Reserved key space for client options.");

    public static final ConfigOption<String> SQL_CURRENT_CATALOG =
            ConfigOptions.key("sql.current-catalog")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Semantically equal to USE CATALOG '...' in SQL. Defines the current catalog. "
                                    + "It is required if object identifiers are not fully qualified.")
                    // Used during EA, can be dropped after OP
                    .withDeprecatedKeys("catalog");

    public static final ConfigOption<String> SQL_CURRENT_DATABASE =
            ConfigOptions.key("sql.current-database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Semantically equal to USE '...' in SQL. Defines the current database. "
                                    + "It is required if object identifiers are not fully qualified.")
                    // Used during EA, can be dropped after OP
                    .withDeprecatedKeys("default_database", "database");

    public static final ConfigOption<Duration> SQL_STATE_TTL =
            ConfigOptions.key("sql.state-ttl")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            "Defines a minimum time interval for how long idle state (i.e. state "
                                    + "that hasn't been updated) will be retained. The system decides "
                                    + "about actual clearance after the given interval. No clearance "
                                    + "is performed if the value is 0 (default).");

    public static final ConfigOption<String> SQL_LOCAL_TIME_ZONE =
            ConfigOptions.key("sql.local-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription(
                            "TIMESTAMP_LTZ values are always transmitted and persisted in UTC. This option "
                                    + "defines the local time zone that should be used when converting between TIMESTAMP_LTZ "
                                    + "and TIMESTAMP/STRING. This includes printing in foreground queries. The value should "
                                    + "be a Time Zone Database (TZDB) ID such as 'America/Los_Angeles' to include daylight "
                                    + "saving time. Fixed offsets are supported using 'GMT-03:00' or 'GMT+03:00'. "
                                    + "'UTC' is used by default.");

    // --------------------------------------------------------------------------------------------
    // Global table options
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

    public static final Set<ConfigOption<?>> PUBLIC_OPTIONS = initPublicOptions();
    public static final ConfigOption<Boolean> CONFLUENT_AI_FUNCTIONS_ENABLED =
            ConfigOptions.key(PRIVATE_PREFIX + "ai-functions.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("A flag to enable or disable Confluent AI functions.");

    private static Set<ConfigOption<?>> initPublicOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CLIENT);
        options.add(SQL_CURRENT_CATALOG);
        options.add(SQL_CURRENT_DATABASE);
        options.add(SQL_STATE_TTL);
        options.add(SQL_LOCAL_TIME_ZONE);
        options.add(SQL_TABLES_SCAN_IDLE_TIMEOUT);
        options.add(SQL_TABLES_SCAN_STARTUP_MODE);
        options.add(SQL_TABLES_SCAN_STARTUP_MILLIS);
        options.add(SQL_TABLES_SCAN_BOUNDED_MODE);
        options.add(SQL_TABLES_SCAN_BOUNDED_MILLIS);
        return Collections.unmodifiableSet(options);
    }

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

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

    private ServiceTasksOptions() {
        // No instantiation
    }
}
