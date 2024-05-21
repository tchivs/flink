/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import io.confluent.flink.table.infoschema.InfoSchemaTables;

import java.util.Collections;
import java.util.Set;

/** {@link DynamicTableSourceFactory} for powering {@link InfoSchemaTables}. */
@Confluent
public class InfoSchemaTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "cc-info-schema-source";

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
    public DynamicTableSource createDynamicTableSource(Context context) {
        // The object name clearly identifies the requested INFORMATION_SCHEMA table
        return new InfoSchemaTableSource(context.getObjectIdentifier().getObjectName());
    }
}
