/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTIONS_PREFIX;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ARGUMENT_TYPES_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CATALOG_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_CLASS_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_DATABASE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ENV_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_IS_DETERMINISTIC_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_NAME_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_ORG_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.FUNCTION_RETURN_TYPE_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.PLUGIN_ID_FIELD;
import static io.confluent.flink.table.modules.remoteudf.UdfUtil.PLUGIN_VERSION_ID_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests {@link UdfUtil}. */
public class UdfUtilTest {
    private String name;

    @BeforeEach
    public void before() {
        name = UdfUtil.createUdfName("cat1", "db1", "func");
    }

    @Test
    public void testNotConfigured() {
        List<ConfiguredRemoteScalarFunction> functions = UdfUtil.extractUdfs(ImmutableMap.of());
        assertThat(functions).hasSize(0);
    }

    private Configuration createConfig() {
        Configuration udfConf = new Configuration();
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + FUNCTION_ORG_FIELD, "testOrg");
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + FUNCTION_ENV_FIELD, "testEnv");
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + FUNCTION_CATALOG_FIELD, "cat1");
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + FUNCTION_DATABASE_FIELD, "db1");
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + FUNCTION_NAME_FIELD, "func");
        udfConf.setString(
                FUNCTIONS_PREFIX + name + "." + FUNCTION_ARGUMENT_TYPES_FIELD + ".0", "INTEGER");
        udfConf.setString(
                FUNCTIONS_PREFIX + name + "." + FUNCTION_ARGUMENT_TYPES_FIELD + ".1",
                "STRING;BIGINT");
        udfConf.setString(
                FUNCTIONS_PREFIX + name + "." + FUNCTION_RETURN_TYPE_FIELD + ".0", "STRING");
        udfConf.setString(
                FUNCTIONS_PREFIX + name + "." + FUNCTION_RETURN_TYPE_FIELD + ".1", "INTEGER");
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + PLUGIN_ID_FIELD, "1234");
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + PLUGIN_VERSION_ID_FIELD, "5678");
        udfConf.setString(
                FUNCTIONS_PREFIX + name + "." + FUNCTION_CLASS_NAME_FIELD, "io.confluent.blah1");
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + FUNCTION_IS_DETERMINISTIC_FIELD, "true");
        return udfConf;
    }

    @Test
    public void testAllThere() {
        Configuration udfConf = createConfig();
        List<ConfiguredRemoteScalarFunction> functions = UdfUtil.extractUdfs(udfConf.toMap());
        assertThat(functions).hasSize(1);
        assertThat(functions.get(0).getOrganization()).isEqualTo("testOrg");
        assertThat(functions.get(0).getEnvironment()).isEqualTo("testEnv");
        assertThat(functions.get(0).getFunctionCatalog()).isEqualTo("cat1");
        assertThat(functions.get(0).getFunctionCatalog()).isEqualTo("cat1");
        assertThat(functions.get(0).getFunctionDatabase()).isEqualTo("db1");
        assertThat(functions.get(0).getFunctionName()).isEqualTo("func");
        assertThat(functions.get(0).isDeterministic()).isTrue();
        assertThat(functions.get(0).getConfiguredFunctionSpecs().size()).isEqualTo(2);
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(0).getArgumentTypes())
                .isEqualTo("INTEGER");
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(0).getReturnType())
                .isEqualTo("STRING");
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(0).getPluginId())
                .isEqualTo("1234");
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(0).getClassName())
                .isEqualTo("io.confluent.blah1");
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(1).getArgumentTypes())
                .isEqualTo("STRING;BIGINT");
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(1).getReturnType())
                .isEqualTo("INTEGER");
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(1).getPluginId())
                .isEqualTo("1234");
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(1).getClassName())
                .isEqualTo("io.confluent.blah1");
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(1).isDeterministic()).isTrue();
        Map<String, String> config = UdfUtil.toConfiguration(functions.get(0));
        assertThat(config).containsExactlyInAnyOrderEntriesOf(udfConf.toMap());
    }

    @Test
    public void testMissingName() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + FUNCTION_NAME_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field name");
    }

    @Test
    public void testMissingCatalog() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + FUNCTION_CATALOG_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field catalog");
    }

    @Test
    public void testMissingDatabase() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + FUNCTION_DATABASE_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field database");
    }

    @Test
    public void testMissingPluginId() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + PLUGIN_ID_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field pluginId");
    }

    @Test
    public void testMissingPluginVersionId() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + PLUGIN_VERSION_ID_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field pluginVersionId");
    }

    @Test
    public void testMissingClassName() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + FUNCTION_CLASS_NAME_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field className");
    }

    @Test
    public void testMissingIsDeterministic() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + FUNCTION_IS_DETERMINISTIC_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field isDeterministic");
    }

    @Test
    public void testMissingArgs() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + FUNCTION_ARGUMENT_TYPES_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field argumentTypes");
    }

    @Test
    public void testNoArgIsOk() {
        Configuration udfConf = createConfig();
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + FUNCTION_ARGUMENT_TYPES_FIELD + ".0", "");
        List<ConfiguredRemoteScalarFunction> functions = UdfUtil.extractUdfs(udfConf.toMap());
        assertThat(functions).hasSize(1);
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(0).getArgumentTypes())
                .isEqualTo("");
    }

    @Test
    public void testMissingReturn() {
        Configuration udfConf = createConfig();
        udfConf.removeKey(FUNCTIONS_PREFIX + name + "." + FUNCTION_RETURN_TYPE_FIELD);
        assertThatThrownBy(() -> UdfUtil.extractUdfs(udfConf.toMap()))
                .hasMessageContaining("Didn't find field returnType");
    }

    @Test
    public void testNotDeterministic() {
        Configuration udfConf = createConfig();
        udfConf.setString(FUNCTIONS_PREFIX + name + "." + FUNCTION_IS_DETERMINISTIC_FIELD, "false");
        List<ConfiguredRemoteScalarFunction> functions = UdfUtil.extractUdfs(udfConf.toMap());
        assertThat(functions).hasSize(1);
        assertThat(functions.get(0).getConfiguredFunctionSpecs().get(0).isDeterministic())
                .isFalse();
        assertThat(functions.get(0).isDeterministic()).isFalse();
    }
}
