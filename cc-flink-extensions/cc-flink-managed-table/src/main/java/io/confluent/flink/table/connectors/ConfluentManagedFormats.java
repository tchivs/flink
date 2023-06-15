/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.factories.Factory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Public formats within Confluent managed tables. */
@Confluent
public class ConfluentManagedFormats {

    public static final Map<String, PublicFormat> FORMATS;

    static {
        final Map<String, PublicFormat> formats = new HashMap<>();
        formats.put(PublicRawFormat.IDENTIFIER, PublicRawFormat.INSTANCE);
        formats.put(PublicAvroRegistryFormat.IDENTIFIER, PublicAvroRegistryFormat.INSTANCE);
        FORMATS = Collections.unmodifiableMap(formats);
    }

    public static PublicFormat getPublicFormatFor(String formatIdentifier) {
        final PublicFormat format = FORMATS.get(formatIdentifier);
        if (format == null) {
            throw new ValidationException("Unsupported format: " + formatIdentifier);
        }
        return format;
    }

    /**
     * Basic interface for a public format that serves as an additional layer before going to
     * internal formats.
     */
    interface PublicFormat extends Format, Factory {}

    // --------------------------------------------------------------------------------------------
    // Format: raw
    // --------------------------------------------------------------------------------------------

    /** Publicly exposed format for {@code raw}. */
    public static class PublicRawFormat implements PublicFormat {

        public static final PublicRawFormat INSTANCE = new PublicRawFormat();

        public static final String IDENTIFIER = "raw";

        public static final ConfigOption<Endianness> ENDIANNESS =
                ConfigOptions.key("endianness")
                        .enumType(Endianness.class)
                        .defaultValue(Endianness.BIG_ENDIAN)
                        .withDescription("Defines the endianness for bytes of numeric values.");

        public static final ConfigOption<String> CHARSET =
                ConfigOptions.key("charset")
                        .stringType()
                        .defaultValue(StandardCharsets.UTF_8.displayName())
                        .withDescription("Defines the string charset.");

        @Override
        public String factoryIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            final Set<ConfigOption<?>> options = new HashSet<>();
            options.add(ENDIANNESS);
            options.add(CHARSET);
            return options;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        /** Enum for {@link #ENDIANNESS}. */
        public enum Endianness {
            BIG_ENDIAN("big-endian"),
            LITTLE_ENDIAN("little-endian");

            private final String value;

            Endianness(String value) {
                this.value = value;
            }

            @Override
            public String toString() {
                return value;
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Format: avro-registry
    // --------------------------------------------------------------------------------------------

    /** Publicly exposed format for {@code avro-registry}. */
    public static class PublicAvroRegistryFormat implements PublicFormat {

        public static final PublicAvroRegistryFormat INSTANCE = new PublicAvroRegistryFormat();

        public static final String IDENTIFIER = "avro-registry";

        @Override
        public String factoryIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Collections.emptySet();
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }
    }

    private ConfluentManagedFormats() {
        // no instantiation
    }
}
