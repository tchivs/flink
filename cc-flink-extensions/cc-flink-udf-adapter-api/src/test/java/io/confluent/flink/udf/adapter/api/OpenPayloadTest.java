/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests OpenPayload. */
public class OpenPayloadTest {

    @Test
    public void testOpenNoConfig() throws Throwable {
        byte[] open = writeOpenPayload(null);
        OpenPayload openPayload = OpenPayload.open(open, OpenPayloadTest.class.getClassLoader());
        assertThat(openPayload.getRemoteUdfSpec().getOrganization()).isEqualTo("org");
        assertThat(openPayload.getRemoteUdfSpec().getEnvironment()).isEqualTo("env");
        assertThat(openPayload.getRemoteUdfSpec().getPluginId()).isEqualTo("pluginId");
        assertThat(openPayload.getRemoteUdfSpec().getPluginVersionId())
                .isEqualTo("pluginVersionId");
        assertThat(openPayload.getRemoteUdfSpec().getFunctionClassName())
                .isEqualTo("functionClass");
        assertThat(openPayload.getRemoteUdfSpec().getArgumentTypes())
                .isEqualTo(Arrays.asList(DataTypes.INT()));
        assertThat(openPayload.getRemoteUdfSpec().getReturnType()).isEqualTo(DataTypes.INT());
        assertThat(openPayload.getConfiguration().toMap()).isEqualTo(Collections.emptyMap());
    }

    @Test
    public void testOpenWithConfig() throws Throwable {
        Map<String, String> map =
                ImmutableMap.of(
                        "a", "b",
                        "c", "d");
        byte[] open = writeOpenPayload(Configuration.fromMap(map));
        OpenPayload openPayload = OpenPayload.open(open, OpenPayloadTest.class.getClassLoader());
        assertThat(openPayload.getConfiguration().toMap()).isEqualTo(map);
    }

    public static byte[] writeOpenPayload(Configuration configuration) throws Exception {
        DataOutputSerializer out = new DataOutputSerializer(512);
        List<LogicalType> args = Arrays.asList(new IntType());
        new RemoteUdfSpec(
                        "org",
                        "env",
                        "pluginId",
                        "pluginVersionId",
                        "functionClass",
                        true,
                        DataTypeUtils.toInternalDataType(new IntType()),
                        args.stream()
                                .map(DataTypeUtils::toInternalDataType)
                                .collect(Collectors.toList()))
                .serialize(out);
        if (configuration != null) {
            FlinkConfiguration.serialize(configuration, out);
        }
        return out.getCopyOfBuffer();
    }
}
