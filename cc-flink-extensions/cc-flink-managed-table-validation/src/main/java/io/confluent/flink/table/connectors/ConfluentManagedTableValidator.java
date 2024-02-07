/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.FactoryHelper;

import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;

import io.confluent.flink.table.connectors.ConfluentManagedFormats.PublicAvroRegistryFormat;
import io.confluent.flink.table.connectors.ConfluentManagedFormats.PublicFormat;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CleanupPolicy;
import io.confluent.flink.table.connectors.ConfluentManagedTableOptions.ManagedChangelogMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.CHANGELOG_MODE;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KAFKA_CLEANUP_POLICY;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.KEY_FORMAT;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.PRIVATE_PREFIX;
import static io.confluent.flink.table.connectors.ConfluentManagedTableOptions.VALUE_FORMAT;
import static io.confluent.flink.table.connectors.ConfluentManagedTableUtils.validateDynamicTableParameters;
import static org.apache.flink.configuration.ConfigurationUtils.canBePrefixMap;
import static org.apache.flink.configuration.ConfigurationUtils.filterPrefixMapKey;

/**
 * Validation and enrichment utility for public options coming directly from the user.
 *
 * <p>The utility provides an additional layer before going to {@code ConfluentManagedTableFactory}
 * and internal {@link Format}s. It assumes public {@link ConfluentManagedTableOptions} used with
 * {@link ConfluentManagedFormats.PublicFormat}.
 */
@Confluent
public class ConfluentManagedTableValidator {

    private static final List<String> PRIVATE_PREFIXES;

    static {
        final List<String> prefixes = new ArrayList<>();
        prefixes.add(PRIVATE_PREFIX);

        for (String formatIdentifier : ConfluentManagedFormats.FORMATS.keySet()) {
            prefixes.add(
                    FactoryUtil.getFormatPrefix(KEY_FORMAT, formatIdentifier) + PRIVATE_PREFIX);
        }

        for (String formatIdentifier : ConfluentManagedFormats.FORMATS.keySet()) {
            prefixes.add(
                    FactoryUtil.getFormatPrefix(VALUE_FORMAT, formatIdentifier) + PRIVATE_PREFIX);
        }

        PRIVATE_PREFIXES = Collections.unmodifiableList(prefixes);
    }

    /** Clears maps that contain both private and public options. */
    public static Map<String, String> filterForPublicOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(e -> PRIVATE_PREFIXES.stream().noneMatch(p -> e.getKey().startsWith(p)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** Clears maps that contain both private and public options. */
    public static Map<String, String> filterForPrivateOptions(Map<String, String> options) {
        return options.entrySet().stream()
                .filter(e -> PRIVATE_PREFIXES.stream().anyMatch(p -> e.getKey().startsWith(p)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * This enriches and validates options coming directly from the user for CREATE TABLE.
     *
     * <p>It assumes that only public options enter this method.
     *
     * @see ConfluentManagedTableOptions#PUBLIC_CREATION_OPTIONS
     * @see ConfluentManagedTableOptions#PUBLIC_RUNTIME_OPTIONS
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Map<String, String> validateCreateTableOptions(
            String tableIdentifier,
            List<String> primaryKeys,
            List<String> bucketKeys,
            List<String> physicalColumns,
            Map<String, String> newOptions) {
        // Validate that the key space contains only supported public options (both from
        // connector and format) that can be parsed according to the config option's data type.
        final PublicConfluentManagedTableFactory factory =
                new PublicConfluentManagedTableFactory(tableIdentifier);

        final PublicConfluentManagedHelper helper =
                new PublicConfluentManagedHelper(factory, newOptions);

        helper.getOptionalFormat(KEY_FORMAT);
        helper.getOptionalFormat(VALUE_FORMAT);

        helper.validate();

        final Configuration validatedOptions = helper.getOptions();
        final boolean hasPrimaryKey = !primaryKeys.isEmpty();
        final boolean hasBucketKey = !bucketKeys.isEmpty();

        enrichChangelogMode(validatedOptions, hasPrimaryKey);
        enrichKeyFormat(validatedOptions, hasPrimaryKey, hasBucketKey);

        // Rediscover formats with enriched options
        final Format keyFormat = helper.getOptionalFormat(KEY_FORMAT).orElse(null);
        final Format valueFormat =
                helper.getOptionalFormat(VALUE_FORMAT)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Could not find required scan format '%s'.",
                                                        VALUE_FORMAT.key())));

        validateDynamicTableParameters(
                tableIdentifier,
                primaryKeys,
                bucketKeys,
                physicalColumns,
                validatedOptions,
                keyFormat,
                valueFormat);

        // Explicitly set defaults
        factory.optionalOptions()
                .forEach(
                        option -> {
                            if (!validatedOptions.getOptional(option).isPresent()
                                    && option.hasDefaultValue()) {
                                validatedOptions.set((ConfigOption) option, option.defaultValue());
                            }
                        });

        return validatedOptions.toMap();
    }

    /**
     * This enriches and validates merged options coming from the user and metastore for ALTER
     * TABLE.
     *
     * <p>It assumes that only public options enter this method.
     *
     * @see ConfluentManagedTableOptions#PUBLIC_CREATION_OPTIONS
     * @see ConfluentManagedTableOptions#PUBLIC_RUNTIME_OPTIONS
     */
    public static Map<String, String> validateAlterTableOptions(
            String tableIdentifier,
            List<String> primaryKeys,
            List<String> bucketKeys,
            List<String> physicalColumns,
            Map<String, String> oldOptions,
            Map<String, String> newOptions) {
        final Map<String, String> validatedOptions =
                validateCreateTableOptions(
                        tableIdentifier, primaryKeys, bucketKeys, physicalColumns, newOptions);
        final Set<String> immutableKeys =
                ConfluentManagedTableOptions.PUBLIC_IMMUTABLE_OPTIONS.stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toSet());
        final Set<String> keyDiff =
                Maps.difference(oldOptions, validatedOptions).entriesDiffering().keySet();
        if (keyDiff.stream().anyMatch(immutableKeys::contains)) {
            throw new ValidationException(
                    String.format(
                            String.format(
                                    "Unsupported modification found for '%s'.\n\n"
                                            + "Unsupported options:\n\n"
                                            + "%s",
                                    tableIdentifier,
                                    keyDiff.stream().sorted().collect(Collectors.joining("\n")))));
        }
        return validatedOptions;
    }

    private static void enrichChangelogMode(Configuration validatedOptions, boolean hasPrimaryKey) {
        if (validatedOptions.getOptional(CHANGELOG_MODE).isPresent()) {
            return;
        }
        if (hasPrimaryKey
                || validatedOptions.get(KAFKA_CLEANUP_POLICY) == CleanupPolicy.COMPACT
                || validatedOptions.get(KAFKA_CLEANUP_POLICY) == CleanupPolicy.DELETE_COMPACT) {
            validatedOptions.set(CHANGELOG_MODE, ManagedChangelogMode.UPSERT);
        } else {
            validatedOptions.set(CHANGELOG_MODE, ManagedChangelogMode.APPEND);
        }
    }

    private static void enrichKeyFormat(
            Configuration validatedOptions, boolean hasPrimaryKey, boolean hasBucketKey) {
        if (validatedOptions.getOptional(KEY_FORMAT).isPresent()
                || (!hasPrimaryKey && !hasBucketKey)) {
            return;
        }
        validatedOptions.set(KEY_FORMAT, PublicAvroRegistryFormat.IDENTIFIER);
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    /**
     * {@link DynamicTableFactory} used to validate public options and to not expose internal
     * options.
     */
    private static class PublicConfluentManagedTableFactory implements DynamicTableFactory {

        private final String tableIdentifier;

        private PublicConfluentManagedTableFactory(String tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
        }

        @Override
        public String factoryIdentifier() {
            // This identifier is not used for discovery, but error messages.
            return tableIdentifier;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            final Set<ConfigOption<?>> options = new HashSet<>();
            options.add(ConfluentManagedTableOptions.CONNECTOR);
            options.addAll(ConfluentManagedTableOptions.PUBLIC_RUNTIME_OPTIONS);
            options.addAll(ConfluentManagedTableOptions.PUBLIC_CREATION_OPTIONS);
            return options;
        }
    }

    /**
     * A modified version of {@link FactoryUtil.TableFactoryHelper} without the need of a {@link
     * DynamicTableFactory.Context} and with support for {@link ConfluentManagedFormats}.
     */
    private static class PublicConfluentManagedHelper
            extends FactoryHelper<PublicConfluentManagedTableFactory> {

        PublicConfluentManagedHelper(
                PublicConfluentManagedTableFactory factory, Map<String, String> configuration) {
            super(factory, configuration);
        }

        Optional<PublicFormat> getOptionalFormat(ConfigOption<String> formatOption) {
            final String identifier = allOptions.get(formatOption);
            if (identifier == null) {
                return Optional.empty();
            }
            final PublicFormat format = ConfluentManagedFormats.getPublicFormatFor(identifier);
            String formatPrefix = formatPrefix(format, formatOption);

            // log all used options of other factories
            final List<ConfigOption<?>> consumedOptions = new ArrayList<>();
            consumedOptions.addAll(format.requiredOptions());
            consumedOptions.addAll(format.optionalOptions());

            consumedOptions.stream()
                    .flatMap(
                            option ->
                                    allKeysExpanded(formatPrefix, option, allOptions.keySet())
                                            .stream())
                    .forEach(consumedOptionKeys::add);

            consumedOptions.stream()
                    .flatMap(this::deprecatedKeys)
                    .map(k -> formatPrefix + k)
                    .forEach(deprecatedOptionKeys::add);

            return Optional.of(format);
        }

        @Override
        public Configuration getOptions() {
            return allOptions;
        }

        private String formatPrefix(Factory formatFactory, ConfigOption<String> formatOption) {
            String identifier = formatFactory.factoryIdentifier();
            return FactoryUtil.getFormatPrefix(formatOption, identifier);
        }

        private Set<String> allKeysExpanded(
                String prefix, ConfigOption<?> option, Set<String> actualKeys) {
            final Set<String> staticKeys =
                    allKeys(option).map(k -> prefix + k).collect(Collectors.toSet());
            if (!canBePrefixMap(option)) {
                return staticKeys;
            }
            // include all prefix keys of a map option by considering the actually provided keys
            return Stream.concat(
                            staticKeys.stream(),
                            staticKeys.stream()
                                    .flatMap(
                                            k ->
                                                    actualKeys.stream()
                                                            .filter(c -> filterPrefixMapKey(k, c))))
                    .collect(Collectors.toSet());
        }

        private Stream<String> allKeys(ConfigOption<?> option) {
            return Stream.concat(Stream.of(option.key()), fallbackKeys(option));
        }

        private Stream<String> fallbackKeys(ConfigOption<?> option) {
            return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                    .map(FallbackKey::getKey);
        }

        private Stream<String> deprecatedKeys(ConfigOption<?> option) {
            return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                    .filter(FallbackKey::isDeprecated)
                    .map(FallbackKey::getKey);
        }
    }
}
