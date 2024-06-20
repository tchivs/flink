/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.providers;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.modules.ml.MLFunctionMetrics;
import io.confluent.flink.table.utils.ml.ModelOptionsUtils;
import io.confluent.flink.table.utils.secrets.SecretDecrypterProvider;
import io.confluent.flink.table.utils.secrets.SecretDecrypterProviderImpl;

import java.time.Clock;
import java.util.Map;

/** Selector for search providers. */
public class SearchProviderSelector {
    public static SearchRuntimeProvider pickProvider(
            CatalogTable table,
            Map<String, String> configuration,
            MLFunctionMetrics metrics,
            Clock clock) {
        if (table == null) {
            return null;
        }
        String searchProvider = ModelOptionsUtils.getProvider(table.getOptions());
        if (searchProvider.isEmpty()) {
            throw new FlinkRuntimeException("Search PROVIDER option not specified on table");
        }

        final SecretDecrypterProvider secretDecrypterProvider =
                new SecretDecrypterProviderImpl(table, configuration, metrics, clock);

        if (searchProvider.equalsIgnoreCase(SearchSupportedProviders.PINECONE.getProviderName())) {
            // OpenAI through their own API or the Azure OpenAI API,
            // which is just a special case of Open AI.
            return new PineconeProvider(table, secretDecrypterProvider);
        } else {
            throw new UnsupportedOperationException(
                    "Search provider not supported: " + searchProvider);
        }
    }
}
