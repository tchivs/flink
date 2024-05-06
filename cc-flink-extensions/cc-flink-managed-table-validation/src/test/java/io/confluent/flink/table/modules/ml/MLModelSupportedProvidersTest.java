/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for MLModelSupportedProviders. */
public class MLModelSupportedProvidersTest {
    private static Collection<String> providers() {
        return Arrays.stream(MLModelSupportedProviders.values())
                .map(Enum::name)
                .collect(Collectors.toList());
    }

    @ParameterizedTest
    @MethodSource("providers")
    public void testFromString(String provider) {
        MLModelSupportedProviders uppercaseProvider =
                MLModelSupportedProviders.fromString(provider);
        MLModelSupportedProviders lowercaseProvider =
                MLModelSupportedProviders.fromString(provider.toLowerCase(Locale.ROOT));
        assertThat(uppercaseProvider).isEqualTo(lowercaseProvider);
        assertThat(uppercaseProvider.getProviderName()).isEqualTo(uppercaseProvider.name());
    }
}
