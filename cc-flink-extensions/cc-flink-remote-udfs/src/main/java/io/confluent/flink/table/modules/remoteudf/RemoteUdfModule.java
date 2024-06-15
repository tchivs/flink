/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.module.Module;

import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Module for Remote UFDs. */
public class RemoteUdfModule implements Module {

    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key("job.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The name of the job as defined by the FCP");

    public static final String CONFLUENT_REMOTE_UDF_PREFIX = "confluent.remote-udf.";

    public static final ConfigOption<Integer> GATEWAY_SERVICE_DEADLINE_SEC =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "gateway.deadline.sec")
                    .intType()
                    .defaultValue(60)
                    .withDescription(
                            "The deadline when calling to the secure gateway (in seconds)");

    public static final ConfigOption<Long> GATEWAY_RETRY_BACKOFF_MS =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "gateway.retry.backoff.ms")
                    .longType()
                    .defaultValue(Duration.ofSeconds(1).toMillis())
                    .withDescription(
                            "The base amount of time to backoff after a secure gateway invocation failure");

    public static final ConfigOption<Integer> GATEWAY_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "gateway.retry.max.attempts")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The total number of attempts to use when retry invoking the secure gateway");
    public static final ConfigOption<String> CONFLUENT_REMOTE_UDF_APISERVER =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "apiserver")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The target for the ApiServer endpoint.");

    public static final ConfigOption<Long> CONFLUENT_REMOTE_UDF_APISERVER_RETRY_BACKOFF_MS =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "apiserver.retry.backoff.ms")
                    .longType()
                    .defaultValue(Duration.ofSeconds(1).toMillis())
                    .withDescription(
                            "The base amount of time to backoff after a ApiServer endpoint failure");

    public static final ConfigOption<Integer> CONFLUENT_REMOTE_UDF_APISERVER_RETRY_MAX_ATTEMPTS =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "apiserver.retry.max.attempts")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The total number of attempts to use when retry invoking the ApiServer endpoint");

    public static final ConfigOption<String> CONFLUENT_REMOTE_UDF_SHIM_PLUGIN_ID =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "pluginid")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The plugin id of the shim jar used for invocation calls.");

    public static final ConfigOption<String> CONFLUENT_REMOTE_UDF_SHIM_VERSION_ID =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "versionid")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The version id of the shim jar used for invocation calls.");

    public static final ConfigOption<Boolean> CONFLUENT_REMOTE_UDF_ASYNC_ENABLED =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "async.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether the async version of udfs are enabled");

    public static final ConfigOption<Boolean> CONFLUENT_REMOTE_UDF_BATCH_ENABLED =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "batch.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether the batch version of udfs are enabled");

    public static final ConfigOption<Integer> CONFLUENT_REMOTE_UDF_BATCH_SIZE =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "batch.size")
                    .intType()
                    .defaultValue(500)
                    .withDescription("The size of a batch for udf requests.");

    public static final ConfigOption<Duration> CONFLUENT_REMOTE_UDF_BATCH_WAIT_TIME_MS =
            ConfigOptions.key(CONFLUENT_REMOTE_UDF_PREFIX + "batch.wait.time")
                    .durationType()
                    .defaultValue(Duration.ofMillis(500))
                    .withDescription("The wait time for rows to batch");

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
