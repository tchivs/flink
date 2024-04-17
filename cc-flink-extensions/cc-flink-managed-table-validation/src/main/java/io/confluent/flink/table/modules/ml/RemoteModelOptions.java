/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CatalogModel.ModelTask;

import java.util.Set;

import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.ENCRYPT_STRATEGY;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.PROVIDER;
import static io.confluent.flink.table.modules.ml.MLModelCommonConstants.TASK;

/** Base options for remote ML model. */
public abstract class RemoteModelOptions {

    // --------------------------------------------------------------------------------------------
    // PUBLIC - GLOBAL FOR ALL TABLES VIA SET COMMAND
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<ModelTask> TASK =
            ConfigOptions.key(MLModelCommonConstants.TASK)
                    .enumType(ModelTask.class)
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

    private final Set<ConfigOption<?>> requiredToplevelOptions = Set.of(TASK, PROVIDER);

    private final Set<ConfigOption<?>> optionalTopLevelOptions = Set.of(CONFLUENT_ENCRYPT_STRATEGY);

    public Set<ConfigOption<?>> getRequiredToplevelOptions() {
        return requiredToplevelOptions;
    }

    public Set<ConfigOption<?>> getOptionalTopLevelOptions() {
        return optionalTopLevelOptions;
    }

    public abstract Set<ConfigOption<?>> getRequiredProviderLevelOptions();

    public abstract Set<ConfigOption<?>> getOptionalProviderLevelOptions();

    public abstract Set<ConfigOption<?>> getSecretOptions();

    public abstract String getparamsPrefix();
}
