/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CatalogTable} that splits public options and private options.
 *
 * <p>Only public options are exposed to the planner. Private options are forwarded under the hood.
 * Once the planning has been completed, private options can be exposed using {@link
 * #exposePrivateOptions(Map)} for serializing them into a {@link CompiledPlan}.
 */
@Confluent
public class ConfluentCatalogTable implements CatalogTable {

    private final Schema schema;
    private final @Nullable String comment;
    private final List<String> partitionKeys;
    private final Map<String, String> publicOptions;
    private final Map<String, String> privateOptions;

    private final Map<String, String> exposedOptions;

    public ConfluentCatalogTable(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> publicOptions,
            Map<String, String> privateOptions) {
        this.schema = checkNotNull(schema, "Schema must not be null.");
        this.comment = comment;
        this.partitionKeys = checkNotNull(partitionKeys, "Partition keys must not be null.");
        this.publicOptions = checkNotNull(publicOptions, "Public options must not be null.");
        this.privateOptions = checkNotNull(privateOptions, "Private options must not be null.");

        checkArgument(
                Stream.concat(publicOptions.entrySet().stream(), privateOptions.entrySet().stream())
                        .allMatch(e -> e.getKey() != null && e.getValue() != null),
                "Options cannot have null keys or values.");

        this.exposedOptions = new HashMap<>(publicOptions);
    }

    /**
     * Exposes private options to {@link #getOptions()} for serializing them into a {@link
     * CompiledPlan}.
     */
    public void exposePrivateOptions(Map<String, String> morePrivateOptions) {
        this.exposedOptions.putAll(privateOptions);
        this.exposedOptions.putAll(morePrivateOptions);
    }

    public Map<String, String> getPrivateOptions() {
        return privateOptions;
    }

    @Override
    public Schema getUnresolvedSchema() {
        return schema;
    }

    @Override
    public String getComment() {
        return comment != null ? comment : "";
    }

    @Override
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    @Override
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @Override
    public Map<String, String> getOptions() {
        return exposedOptions;
    }

    @Override
    public CatalogBaseTable copy() {
        return new ConfluentCatalogTable(
                schema, comment, partitionKeys, publicOptions, privateOptions);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new ConfluentCatalogTable(schema, comment, partitionKeys, options, privateOptions);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public Map<String, String> toProperties() {
        throw new UnsupportedOperationException(
                "Only a resolved catalog table can be serialized into a map of string properties.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ConfluentCatalogTable that = (ConfluentCatalogTable) o;
        return schema.equals(that.schema)
                && Objects.equals(comment, that.comment)
                && partitionKeys.equals(that.partitionKeys)
                && publicOptions.equals(that.publicOptions)
                && privateOptions.equals(that.privateOptions)
                && exposedOptions.equals(that.exposedOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema, comment, partitionKeys, publicOptions, privateOptions, exposedOptions);
    }
}
