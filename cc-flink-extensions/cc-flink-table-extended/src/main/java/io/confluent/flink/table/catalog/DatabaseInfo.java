/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;

/**
 * Pair of database ID (e.g. local Kafka cluster ID) and database name (e.g. cluster display name).
 */
@Confluent
public class DatabaseInfo {

    private final String id;

    private final String name;

    private DatabaseInfo(String id, String name) {
        this.id = id;
        this.name = name;
    }

    public static DatabaseInfo of(String id, String name) {
        return new DatabaseInfo(id, name);
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
