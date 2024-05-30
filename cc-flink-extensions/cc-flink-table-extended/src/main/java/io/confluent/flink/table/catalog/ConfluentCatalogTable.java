/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.TableDistribution;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Confluent-specific {@link CatalogTable} that ensures the table comes from metastore.
 *
 * @see ConfluentCatalog
 */
@Confluent
public class ConfluentCatalogTable implements CatalogTable {

    private final Schema schema;
    private final @Nullable String comment;
    private final @Nullable TableDistribution distribution;
    private final List<String> partitionKeys;
    private final Map<String, String> options;

    public ConfluentCatalogTable(
            Schema schema,
            @Nullable String comment,
            @Nullable TableDistribution distribution,
            List<String> partitionKeys,
            Map<String, String> options) {
        this.schema = checkNotNull(schema, "Schema must not be null.");
        this.comment = comment;
        this.distribution = distribution;
        this.partitionKeys = checkNotNull(partitionKeys, "Partition keys must not be null.");
        this.options = new HashMap<>(checkNotNull(options, "Options must not be null."));

        checkArgument(
                options.entrySet().stream()
                        .allMatch(e -> e.getKey() != null && e.getValue() != null),
                "Options cannot have null keys or values.");
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
    public Optional<TableDistribution> getDistribution() {
        return Optional.ofNullable(distribution);
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public CatalogBaseTable copy() {
        return new ConfluentCatalogTable(schema, comment, distribution, partitionKeys, options);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new ConfluentCatalogTable(schema, comment, distribution, partitionKeys, options);
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
                && Objects.equals(distribution, that.distribution)
                && partitionKeys.equals(that.partitionKeys)
                && options.equals(that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, comment, distribution, partitionKeys, options);
    }
}
