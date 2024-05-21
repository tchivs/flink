/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link TShirtSizingIsSmaller}. */
public class TShirtSizingIsSmallerTest {

    private TShirtSizingIsSmaller udf = new TShirtSizingIsSmaller();

    @BeforeEach
    public void before() {}

    @Test
    public void testIsSmaller() {
        assertThat(udf.eval("X-Small", "Small")).isTrue();
        assertThat(udf.eval("XS", "S")).isTrue();
        assertThat(udf.eval("Small", "Medium")).isTrue();
        assertThat(udf.eval("S", "M")).isTrue();
        assertThat(udf.eval("Medium", "Large")).isTrue();
        assertThat(udf.eval("M", "L")).isTrue();
        assertThat(udf.eval("Large", "X-Large")).isTrue();
        assertThat(udf.eval("L", "XL")).isTrue();
        assertThat(udf.eval("X-Large", "XX-Large")).isTrue();
        assertThat(udf.eval("XL", "XXL")).isTrue();
        assertThat(udf.eval("Small", "X-Small")).isFalse();
        assertThat(udf.eval("Medium", "Small")).isFalse();
        assertThat(udf.eval("Large", "Medium")).isFalse();
        assertThat(udf.eval("X-Large", "Large")).isFalse();

        // Skipping levels
        assertThat(udf.eval("X-Small", "Medium")).isTrue();
        assertThat(udf.eval("XS", "M")).isTrue();
        assertThat(udf.eval("X-Small", "Large")).isTrue();
        assertThat(udf.eval("XS", "L")).isTrue();
        assertThat(udf.eval("Small", "Large")).isTrue();
        assertThat(udf.eval("S", "L")).isTrue();
        assertThat(udf.eval("Small", "Large")).isTrue();
        assertThat(udf.eval("Medium", "X-Large")).isTrue();
        assertThat(udf.eval("M", "XL")).isTrue();
        assertThat(udf.eval("Medium", "X-Small")).isFalse();
        assertThat(udf.eval("Large", "Small")).isFalse();
        assertThat(udf.eval("X-Large", "Medium")).isFalse();
    }
}
