/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import io.confluent.flink.table.connectors.ConfluentManagedTableOptions;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.GlobalScanBoundedMode;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.GlobalScanStartupMode;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Supported configuration options for {@link ServiceTasks}. */
@Confluent
public final class ServiceTasksOptions {

    // --------------------------------------------------------------------------------------------
    // Ignored keys
    // --------------------------------------------------------------------------------------------

    public static boolean isIgnored(String key) {
        // Reserved key space for client options.
        final boolean isClientOption = key.startsWith("client.");

        // Invalid option set by client during EA, can be dropped after OP
        final boolean isDeprecatedClientOption = key.equals("table.local-time-zone");

        return isClientOption || isDeprecatedClientOption;
    }

    // --------------------------------------------------------------------------------------------
    // General SQL Options
    // --------------------------------------------------------------------------------------------

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

    public static final ConfigOption<Boolean> SQL_DRY_RUN =
            ConfigOptions.key("sql.dry-run")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Statement submitted with the flag enabled will be parsed and "
                                    + "validated the same way as a statement without the flag. "
                                    + "However, it will not be executed.");

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

    // This will eventually be replaced by a secret store see
    // https://confluentinc.atlassian.net/browse/FRT-256
    public static final ConfigOption<Map<String, String>> SQL_SECRETS =
            ConfigOptions.key("sql.secrets")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription(
                            "User secrets map can be used to access remote services, e.g, "
                                    + "'sql.secrets.openai'");

    // --------------------------------------------------------------------------------------------
    // Global table options
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Duration> SQL_TABLES_SCAN_IDLE_TIMEOUT =
            ConfluentManagedTableOptions.SQL_TABLES_SCAN_IDLE_TIMEOUT;

    public static final ConfigOption<GlobalScanStartupMode> SQL_TABLES_SCAN_STARTUP_MODE =
            ConfluentManagedTableOptions.SQL_TABLES_SCAN_STARTUP_MODE;

    public static final ConfigOption<Long> SQL_TABLES_SCAN_STARTUP_MILLIS =
            ConfluentManagedTableOptions.SQL_TABLES_SCAN_STARTUP_MILLIS;

    public static final ConfigOption<GlobalScanBoundedMode> SQL_TABLES_SCAN_BOUNDED_MODE =
            ConfluentManagedTableOptions.SQL_TABLES_SCAN_BOUNDED_MODE;

    public static final ConfigOption<Long> SQL_TABLES_SCAN_BOUNDED_MILLIS =
            ConfluentManagedTableOptions.SQL_TABLES_SCAN_BOUNDED_MILLIS;

    // --------------------------------------------------------------------------------------------
    // Private Options
    // --------------------------------------------------------------------------------------------

    public static final String PRIVATE_PREFIX = "confluent.";

    public static final ConfigOption<Boolean> CONFLUENT_AI_FUNCTIONS_ENABLED =
            ConfigOptions.key(PRIVATE_PREFIX + "ai-functions.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("A flag to enable or disable Confluent AI functions.");

    public static final ConfigOption<Duration> CONFLUENT_AI_FUNCTIONS_CALL_TIMEOUT =
            ConfigOptions.key(PRIVATE_PREFIX + "ai-functions.call-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("A timeout for Confluent AI function calls.");

    public static final ConfigOption<Boolean> CONFLUENT_REMOTE_UDF_ENABLED =
            ConfigOptions.key(PRIVATE_PREFIX + "remote-udf.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("A flag to enable or disable Confluent remote UDFs.");

    public static final ConfigOption<String> CONFLUENT_REMOTE_UDF_TARGET =
            ConfigOptions.key(PRIVATE_PREFIX + "remote-udf.target")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The target for the remote Udf endpoint.");

    public static final ConfigOption<Boolean> CONFLUENT_OTLP_FUNCTIONS_ENABLED =
            ConfigOptions.key(PRIVATE_PREFIX + "otlp.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("A flag to enable or disable Confluent OTLP functions.");

    // Used by Flink Sql Service - [SQL-1354]
    public static final ConfigOption<Boolean> CONFLUENT_TABLE_ASYNC_ENABLED =
            ConfigOptions.key(PRIVATE_PREFIX + "table-async.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("A flag to enable or disable TableRequest Async.");

    /**
     * Prefix to mark options that have been introduced by Confluent and set by the user. It is used
     * in resources to avoid namespace collision with current or future Flink options.
     */
    public static final String PRIVATE_USER_PREFIX = PRIVATE_PREFIX + "user.";

    // --------------------------------------------------------------------------------------------
    // Stable Public Options
    // --------------------------------------------------------------------------------------------

    public static final Set<ConfigOption<?>> STABLE_PUBLIC_OPTIONS = initStablePublicOptions();

    private static Set<ConfigOption<?>> initStablePublicOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SQL_CURRENT_CATALOG);
        options.add(SQL_CURRENT_DATABASE);
        options.add(SQL_DRY_RUN);
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
    // Early Access Public Options
    // --------------------------------------------------------------------------------------------

    public static final Set<ConfigOption<?>> EARLY_ACCESS_PUBLIC_OPTIONS = initEAPublicOptions();

    private static Set<ConfigOption<?>> initEAPublicOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SQL_SECRETS);
        return Collections.unmodifiableSet(options);
    }

    // --------------------------------------------------------------------------------------------
    // All Public Options
    // --------------------------------------------------------------------------------------------

    public static final Set<ConfigOption<?>> ALL_PUBLIC_OPTIONS = initAllPublicOptions();

    private static Set<ConfigOption<?>> initAllPublicOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.addAll(STABLE_PUBLIC_OPTIONS);
        options.addAll(EARLY_ACCESS_PUBLIC_OPTIONS);
        return Collections.unmodifiableSet(options);
    }

    private ServiceTasksOptions() {
        // No instantiation
    }
}
