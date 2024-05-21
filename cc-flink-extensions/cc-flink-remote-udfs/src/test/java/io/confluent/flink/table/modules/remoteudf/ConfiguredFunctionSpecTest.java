/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests {@link ConfiguredFunctionSpec}. */
public class ConfiguredFunctionSpecTest {

    @BeforeEach
    public void before() {}

    @Test
    public void testConfiguredRight() {
        List<ConfiguredFunctionSpec> spec =
                ConfiguredFunctionSpec.newBuilder()
                        .setOrganization("testOrg")
                        .setEnvironment("testEnv")
                        .setCatalog("cat")
                        .setDatabase("db")
                        .setName("func")
                        .setPluginId("funcId")
                        .setPluginVersionId("versionId")
                        .setClassName("io.confluent.blah")
                        .parseIsDeterministic("true")
                        .addArgumentTypes("INT")
                        .addReturnType("STRING")
                        .build();
        assertThat(spec).hasSize(1);
        assertThat(spec.get(0).getOrganization()).isEqualTo("testOrg");
        assertThat(spec.get(0).getEnvironment()).isEqualTo("testEnv");
        assertThat(spec.get(0).getCatalog()).isEqualTo("cat");
        assertThat(spec.get(0).getDatabase()).isEqualTo("db");
        assertThat(spec.get(0).getName()).isEqualTo("func");
        assertThat(spec.get(0).getPluginId()).isEqualTo("funcId");
        assertThat(spec.get(0).getPluginVersionId()).isEqualTo("versionId");
        assertThat(spec.get(0).getClassName()).isEqualTo("io.confluent.blah");
        assertThat(spec.get(0).getArgumentTypes()).isEqualTo("INT");
        assertThat(spec.get(0).getReturnType()).isEqualTo("STRING");
        assertThat(spec.get(0).isDeterministic()).isTrue();
    }

    @Test
    public void testOverrides() {
        List<ConfiguredFunctionSpec> spec =
                ConfiguredFunctionSpec.newBuilder()
                        .setOrganization("testOrg")
                        .setEnvironment("testEnv")
                        .setCatalog("cat")
                        .setDatabase("db")
                        .setName("func")
                        .setPluginId("funcId")
                        .setPluginVersionId("versionId")
                        .setClassName("io.confluent.blah")
                        .parseIsDeterministic("true")
                        .addArgumentTypes("INT")
                        .addArgumentTypes("STRING")
                        .addReturnType("STRING")
                        .addReturnType("INT")
                        .build();
        assertThat(spec).hasSize(2);
        assertThat(spec.get(0).getOrganization()).isEqualTo("testOrg");
        assertThat(spec.get(0).getEnvironment()).isEqualTo("testEnv");
        assertThat(spec.get(0).getCatalog()).isEqualTo("cat");
        assertThat(spec.get(0).getDatabase()).isEqualTo("db");
        assertThat(spec.get(0).getName()).isEqualTo("func");
        assertThat(spec.get(0).getPluginId()).isEqualTo("funcId");
        assertThat(spec.get(0).getPluginVersionId()).isEqualTo("versionId");
        assertThat(spec.get(0).getClassName()).isEqualTo("io.confluent.blah");
        assertThat(spec.get(0).getArgumentTypes()).isEqualTo("INT");
        assertThat(spec.get(0).getReturnType()).isEqualTo("STRING");
        assertThat(spec.get(0).isDeterministic()).isTrue();
        assertThat(spec.get(1).getCatalog()).isEqualTo("cat");
        assertThat(spec.get(1).getDatabase()).isEqualTo("db");
        assertThat(spec.get(1).getName()).isEqualTo("func");
        assertThat(spec.get(1).getPluginId()).isEqualTo("funcId");
        assertThat(spec.get(1).getPluginVersionId()).isEqualTo("versionId");
        assertThat(spec.get(1).getClassName()).isEqualTo("io.confluent.blah");
        assertThat(spec.get(1).getArgumentTypes()).isEqualTo("STRING");
        assertThat(spec.get(1).getReturnType()).isEqualTo("INT");
        assertThat(spec.get(1).isDeterministic()).isTrue();
    }

    @Test
    public void testInconsistentArgs() {
        ConfiguredFunctionSpec.Builder builder =
                ConfiguredFunctionSpec.newBuilder()
                        .setOrganization("testOrg")
                        .setEnvironment("testEnv")
                        .setCatalog("cat")
                        .setDatabase("db")
                        .setName("func")
                        .setPluginId("funcId")
                        .setPluginVersionId("versionId")
                        .setClassName("io.confluent.blah")
                        .parseIsDeterministic("true")
                        .addArgumentTypes("INT")
                        .addArgumentTypes("STRING")
                        .addReturnType("STRING");
        assertThatThrownBy(builder::build)
                .hasMessageContaining("Args and results should be equal in size");
    }

    @Test
    public void testEmptyArgs() {
        List<ConfiguredFunctionSpec> spec =
                ConfiguredFunctionSpec.newBuilder()
                        .setOrganization("testOrg")
                        .setEnvironment("testEnv")
                        .setCatalog("cat")
                        .setDatabase("db")
                        .setName("func")
                        .setPluginId("funcId")
                        .setPluginVersionId("versionId")
                        .setClassName("io.confluent.blah")
                        .parseIsDeterministic("true")
                        .addArgumentTypes(Collections.emptyList())
                        .addReturnType("STRING")
                        .build();
        assertThat(spec).hasSize(1);
        assertThat(spec.get(0).getOrganization()).isEqualTo("testOrg");
        assertThat(spec.get(0).getEnvironment()).isEqualTo("testEnv");
        assertThat(spec.get(0).getCatalog()).isEqualTo("cat");
        assertThat(spec.get(0).getDatabase()).isEqualTo("db");
        assertThat(spec.get(0).getName()).isEqualTo("func");
        assertThat(spec.get(0).getPluginId()).isEqualTo("funcId");
        assertThat(spec.get(0).getPluginVersionId()).isEqualTo("versionId");
        assertThat(spec.get(0).getClassName()).isEqualTo("io.confluent.blah");
        assertThat(spec.get(0).getArgumentTypes()).isEqualTo("");
        assertThat(spec.get(0).getReturnType()).isEqualTo("STRING");
        assertThat(spec.get(0).isDeterministic()).isTrue();
    }
}
