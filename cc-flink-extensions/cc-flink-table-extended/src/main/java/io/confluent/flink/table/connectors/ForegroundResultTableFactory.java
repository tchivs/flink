/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Set;

/**
 * Confluent factory for representing a result serving sink in a {@link CompiledPlan} for powering
 * foreground queries.
 */
@Confluent
public class ForegroundResultTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "cc-foreground-sink";

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
        return Collections.emptySet();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        helper.validate();

        final String zone = context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE);
        final ZoneId zoneId =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zone);

        return new ForegroundResultTableSink(
                context.getConfiguration().get(MAX_BATCH_SIZE),
                context.getConfiguration().get(SOCKET_TIMEOUT),
                context.getPhysicalRowDataType(),
                zoneId);
    }
}
