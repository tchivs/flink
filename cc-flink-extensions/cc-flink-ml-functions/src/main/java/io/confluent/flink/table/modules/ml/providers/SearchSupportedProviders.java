/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.providers;

import java.util.Locale;

/** Supported providers for search. */
public enum SearchSupportedProviders {
    // TODO: Move this into the search module. It's currently here to avoid circular dependencies on
    // the metrics and decrypter code.
    // TODO: Add endpoints and endpoint validation to the providers.
    PINECONE("PINECONE"),
    ELASTIC("ELASTIC"),
    ;

    private final String providerName;

    SearchSupportedProviders(String providerName) {
        this.providerName = providerName;
    }

    public String getProviderName() {
        return providerName;
    }

    public static SearchSupportedProviders fromString(String providerName) {
        return SearchSupportedProviders.valueOf(providerName.toUpperCase(Locale.ROOT));
    }
}
