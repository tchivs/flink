/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogView;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Confluent-specific {@link CatalogView} that ensures the view comes from metastore.
 *
 * @see ConfluentCatalog
 */
@Confluent
public class ConfluentCatalogView implements CatalogView {
    private final Schema schema;
    private final @Nullable String comment;
    private final String viewQuery;
    private final String expandedViewQuery;

    public ConfluentCatalogView(
            Schema schema, @Nullable String comment, String viewQuery, String expandedViewQuery) {
        this.schema = schema;
        this.comment = comment;
        this.viewQuery = viewQuery;
        this.expandedViewQuery = expandedViewQuery;
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
    public String getOriginalQuery() {
        return viewQuery;
    }

    @Override
    public String getExpandedQuery() {
        return expandedViewQuery;
    }

    @Override
    public Map<String, String> getOptions() {
        return Collections.emptyMap();
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
    public CatalogBaseTable copy() {
        return new ConfluentCatalogView(schema, comment, viewQuery, expandedViewQuery);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConfluentCatalogView that = (ConfluentCatalogView) o;
        return Objects.equals(schema, that.schema)
                && Objects.equals(comment, that.comment)
                && Objects.equals(viewQuery, that.viewQuery)
                && Objects.equals(expandedViewQuery, that.expandedViewQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, comment, viewQuery, expandedViewQuery);
    }
}
