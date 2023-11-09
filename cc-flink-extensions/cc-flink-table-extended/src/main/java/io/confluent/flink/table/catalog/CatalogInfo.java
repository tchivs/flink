/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;

/** Pair of catalog ID (e.g. environment ID) and catalog name (e.g. environment display name). */
@Confluent
public class CatalogInfo {

    private final String id;

    private final String name;

    private CatalogInfo(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public static CatalogInfo of(String id, String name) {
        return new CatalogInfo(id, name);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return id;
    }
}
