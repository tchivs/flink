/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.catalog;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

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
        return new DatabaseInfo(
                Preconditions.checkNotNull(id, "id"), Preconditions.checkNotNull(name, "name"));
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DatabaseInfo that = (DatabaseInfo) o;
        return id.equals(that.id) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return id;
    }
}
