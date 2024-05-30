/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.flink.table.modules.ml.secrets;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.catalog.CatalogModel;

import cloud.confluent.ksql_api_service.flinkcredential.FlinkCredentialServiceGrpc.FlinkCredentialServiceBlockingStub;
import io.confluent.flink.credentials.utils.CallWithRetry;
import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.modules.ml.providers.MLModelSupportedProviders;
import io.confluent.flink.table.utils.mlutils.ModelOptionsUtils;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Decrypter for model secrets. */
public class ModelSecretDecrypter extends FlinkCredentialServiceSecretDecrypter<CatalogModel> {
    private ModelOptionsUtils modelOptionsUtils;

    public ModelSecretDecrypter(CatalogModel model, Map<String, String> configuration) {
        super(model, configuration);
    }

    @VisibleForTesting
    public ModelSecretDecrypter(
            CatalogModel model,
            Map<String, String> configuration,
            FlinkCredentialServiceBlockingStub stub,
            Function<String, byte[]> dataSigner,
            CallWithRetry callWithRetry) {
        super(model, configuration, stub, dataSigner, callWithRetry);
    }

    @Override
    String getSecretValue(String secretKey) {
        return modelOptionsUtils.getOption(secretKey);
    }

    @Override
    public String getProviderName() {
        return MLModelSupportedProviders.fromString(modelOptionsUtils.getProvider())
                .getProviderName();
    }

    @Override
    ParsedConfig configure(CatalogModel model, Map<String, String> configuration) {
        modelOptionsUtils = new ModelOptionsUtils(model.getOptions());

        ParsedConfig parsedConfig = new ParsedConfig();
        parsedConfig.orgId =
                Objects.requireNonNull(
                        configuration.getOrDefault(
                                MLModelCommonConstants.ORG_ID,
                                modelOptionsUtils.getOption(MLModelCommonConstants.ORG_ID)),
                        MLModelCommonConstants.ORG_ID);
        parsedConfig.computePoolId =
                Objects.requireNonNull(
                        configuration.getOrDefault(
                                MLModelCommonConstants.COMPUTE_POOL_ID,
                                modelOptionsUtils.getOption(
                                        MLModelCommonConstants.COMPUTE_POOL_ID)),
                        MLModelCommonConstants.COMPUTE_POOL_ID);
        parsedConfig.computePoolEnvId =
                Objects.requireNonNull(
                        configuration.getOrDefault(
                                MLModelCommonConstants.COMPUTE_POOL_ENV_ID,
                                modelOptionsUtils.getOption(
                                        MLModelCommonConstants.COMPUTE_POOL_ENV_ID)),
                        MLModelCommonConstants.COMPUTE_POOL_ENV_ID);
        parsedConfig.encryptStrategy =
                Objects.requireNonNull(
                        modelOptionsUtils.getEncryptStrategy(), "EncryptionStrategy");

        parsedConfig.credentialServiceHost =
                Objects.requireNonNull(
                        configuration.getOrDefault(
                                MLModelCommonConstants.CREDENTIAL_SERVICE_HOST,
                                modelOptionsUtils.getCredentialServiceHost()));
        String port = configuration.get(MLModelCommonConstants.CREDENTIAL_SERVICE_PORT);
        parsedConfig.credentialServicePort =
                port == null
                        ? modelOptionsUtils.getCredentialServicePort()
                        : Integer.parseInt(port);

        // Below are optional fields
        parsedConfig.resourceEnvId =
                configuration.getOrDefault(
                        MLModelCommonConstants.ENV_ID,
                        modelOptionsUtils.getOptionOrDefault(MLModelCommonConstants.ENV_ID, ""));
        parsedConfig.databaseId =
                configuration.getOrDefault(
                        MLModelCommonConstants.DATABASE_ID,
                        modelOptionsUtils.getOptionOrDefault(
                                MLModelCommonConstants.DATABASE_ID, ""));
        parsedConfig.resourceName =
                configuration.getOrDefault(
                        MLModelCommonConstants.MODEL_NAME,
                        modelOptionsUtils.getOptionOrDefault(
                                MLModelCommonConstants.MODEL_NAME, ""));
        parsedConfig.resourceVersion =
                configuration.getOrDefault(
                        MLModelCommonConstants.MODEL_VERSION,
                        modelOptionsUtils.getOptionOrDefault(
                                MLModelCommonConstants.MODEL_VERSION, ""));

        String retryCount = configuration.get(MLModelCommonConstants.KMS_SECRET_RETRY_COUNT);
        parsedConfig.maxAttempt =
                retryCount == null
                        ? FlinkCredentialServiceSecretDecrypter.MAX_ATTEMPT_DEFAULT
                        : Integer.parseInt(retryCount);

        String delayMs = configuration.get(MLModelCommonConstants.KMS_SECRET_RETRY_DELAY_MS);
        parsedConfig.delayMs =
                delayMs == null
                        ? FlinkCredentialServiceSecretDecrypter.DELAY_MS_DEFAULT
                        : Integer.parseInt(delayMs);
        return parsedConfig;
    }
}
