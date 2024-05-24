/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.ENCRYPT_STRATEGY;

/** Base options for remote ML model. */
public abstract class RemoteModelOptions {

    // --------------------------------------------------------------------------------------------
    // PUBLIC - TOP LEVEL CONFIG
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<MLModelCommonConstants.ModelTask> TASK =
            ConfigOptions.key(MLModelCommonConstants.TASK)
                    .enumType(MLModelCommonConstants.ModelTask.class)
                    .noDefaultValue()
                    .withDescription("The task type of the remote ML model.");

    public static final ConfigOption<MLModelSupportedProviders> PROVIDER =
            ConfigOptions.key(MLModelCommonConstants.PROVIDER)
                    .enumType(MLModelSupportedProviders.class)
                    .noDefaultValue()
                    .withDescription("The provider of the remote ML model.");

    // --------------------------------------------------------------------------------------------
    // PRIVATE - CONFLUENT MODEL CONFIG - SET BY SQL SERVICE
    // --------------------------------------------------------------------------------------------
    public static final ConfigOption<EncryptionStrategy> CONFLUENT_ENCRYPT_STRATEGY =
            ConfigOptions.key(ENCRYPT_STRATEGY)
                    .enumType(EncryptionStrategy.class)
                    .noDefaultValue()
                    .withDescription(
                            "The encryption strategy for the model secret,"
                                    + "it can only be set by Confluent Service");

    // --------------------------------------------------------------------------------------------
    // ENUM
    // --------------------------------------------------------------------------------------------

    /** Secrets encryption strategy. */
    public enum EncryptionStrategy {
        PLAINTEXT("plaintext"),
        KMS("kms");
        private final String value;

        EncryptionStrategy(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private final Set<ConfigOption<?>> requiredToplevelOptions = ImmutableSet.of(TASK, PROVIDER);

    private final Set<ConfigOption<?>> publicOptionalTopLevelOptions = ImmutableSet.of();

    public Set<ConfigOption<?>> getRequiredToplevelOptions() {
        return requiredToplevelOptions;
    }

    public Set<ConfigOption<?>> getPublicOptionalTopLevelOptions() {
        return publicOptionalTopLevelOptions;
    }

    public abstract Set<ConfigOption<?>> getRequiredProviderLevelOptions();

    public abstract Set<ConfigOption<?>> getOptionalProviderLevelOptions();

    public abstract Set<ConfigOption<?>> getSecretOptions();

    public abstract String getParamsPrefix();

    public abstract void validateEndpoint(Configuration configuration, boolean runtime);

    public abstract MLModelSupportedProviders getProvider();
}
