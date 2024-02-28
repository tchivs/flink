/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Module for Remote UFDs. */
public class RemoteUdfModule implements Module {
    public static final ConfigOption<String> CONFLUENT_REMOTE_UDF_TARGET =
            ConfigOptions.key("confluent.remote-udf.target")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The target for the remote Udf endpoint.");

    private final Map<String, FunctionDefinition> normalizedFunctions;

    public RemoteUdfModule() {
        // Register all the UDFs as system function under the name for
        // testing purposes.
        normalizedFunctions =
                new HashMap<String, FunctionDefinition>() {
                    {
                        put(TShirtSizingIsSmaller.NAME, new TShirtSizingIsSmaller());
                    }
                };
    }

    @Override
    public Set<String> listFunctions() {
        return normalizedFunctions.keySet();
    }

    @Override
    public Optional<FunctionDefinition> getFunctionDefinition(String name) {
        final String normalizedName = name.toUpperCase(Locale.ROOT);
        return Optional.ofNullable(normalizedFunctions.get(normalizedName));
    }
}
