/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Confluent factory for representing a result serving sink in a {@link CompiledPlan} for powering
 * foreground queries.
 */
public class ForegroundDynamicTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "cc-foreground";

    public static final ConfigOption<MemorySize> MAX_BATCH_SIZE =
            ConfigOptions.key("batch-size-max")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(2));
    public static final ConfigOption<Duration> SOCKET_TIMEOUT =
            ConfigOptions.key("socket-timeout").durationType().defaultValue(Duration.ofSeconds(10));

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
        options.add(MAX_BATCH_SIZE);
        options.add(SOCKET_TIMEOUT);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        return new ForegroundDynamicSink(
                helper.getOptions().get(MAX_BATCH_SIZE),
                helper.getOptions().get(SOCKET_TIMEOUT),
                context.getPhysicalRowDataType());
    }
}
