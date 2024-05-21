/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.Objects;

/** Metadata that configures a connector (i.e. a table source and/or table sink). */
@Confluent
public class ConnectorMetadata {

    private final ObjectIdentifier identifier;

    private final Map<String, String> allOptions;

    public ConnectorMetadata(ObjectIdentifier identifier, Map<String, String> allOptions) {
        this.identifier = Preconditions.checkNotNull(identifier, "Identifier must not be null.");
        this.allOptions = Preconditions.checkNotNull(allOptions, "Options must not be null.");
    }

    public ObjectIdentifier getIdentifier() {
        return identifier;
    }

    /** All options (public and private ones) required to configure a connector. */
    public Map<String, String> getAllOptions() {
        return allOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectorMetadata that = (ConnectorMetadata) o;
        return identifier.equals(that.identifier) && allOptions.equals(that.allOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, allOptions);
    }
}
