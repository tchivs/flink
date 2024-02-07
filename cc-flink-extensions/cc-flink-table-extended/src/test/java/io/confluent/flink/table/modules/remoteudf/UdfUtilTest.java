/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTIONS_PREFIX;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ARGUMENT_TYPES_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CATALOG_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CLASS_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_DATABASE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ID_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_RETURN_TYPE_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests {@link UdfUtil}. */
public class UdfUtilTest {

    @BeforeEach
    public void before() {}

    @Test
    public void testNotConfigured() {
        List<ConfiguredRemoteScalarFunction> functions = UdfUtil.extractUdfs(ImmutableMap.of());
        assertThat(functions).hasSize(0);
    }

    private Configuration createConfig() {
        Configuration udfConf = new Configuration();
        udfConf.setString(FUNCTIONS_PREFIX + "a." + FUNCTION_CATALOG_FIELD, "cat1");
        udfConf.setString(FUNCTIONS_PREFIX + "a." + FUNCTION_DATABASE_FIELD, "db1");
        udfConf.setString(FUNCTIONS_PREFIX + "a." + FUNCTION_NAME_FIELD, "func");
        udfConf.setString(
                FUNCTIONS_PREFIX + "a." + FUNCTION_ARGUMENT_TYPES_FIELD + ".0", "INTEGER");
        udfConf.setString(FUNCTIONS_PREFIX + "a." + FUNCTION_RETURN_TYPE_FIELD + ".0", "STRING");
        udfConf.setString(FUNCTIONS_PREFIX + "a." + FUNCTION_ID_FIELD, "1234");
        udfConf.setString(
                FUNCTIONS_PREFIX + "a." + FUNCTION_CLASS_NAME_FIELD, "io.confluent.blah1");
        return udfConf;
    }

    @Test
    public void testAllThere() {
        Configuration udfConf = createConfig();
        List<ConfiguredRemoteScalarFunction> functions = UdfUtil.extractUdfs(udfConf.toMap());
        assertThat(functions).hasSize(1);
    }

    @Test
    public void testMissingName() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + "a." + FUNCTION_NAME_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()));
    }

    @Test
    public void testMissingCatalog() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + "a." + FUNCTION_CATALOG_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()));
    }

    @Test
    public void testMissingDatabase() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + "a." + FUNCTION_DATABASE_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()));
    }

    @Test
    public void testMissingFunctionId() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + "a." + FUNCTION_ID_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()));
    }

    @Test
    public void testMissingClassName() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + "a." + FUNCTION_CLASS_NAME_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()));
    }

    @Test
    public void testMissingArgs() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + "a." + FUNCTION_ARGUMENT_TYPES_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()));
    }

    @Test
    public void testMissingReturn() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + "a." + FUNCTION_RETURN_TYPE_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()));
    }
}
