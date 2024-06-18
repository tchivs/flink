/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link VariableNameUtil}. */
class VariableNameUtilTest {

    @Test
    void getVariableName() {
        Assertions.assertEquals("t", VariableNameUtil.getVariableName("<t>"));
        Assertions.assertEquals("<t", VariableNameUtil.getVariableName("<t"));
        Assertions.assertEquals("t>", VariableNameUtil.getVariableName("t>"));
        Assertions.assertEquals("<", VariableNameUtil.getVariableName("<"));
        Assertions.assertEquals(">", VariableNameUtil.getVariableName(">"));
        Assertions.assertEquals("", VariableNameUtil.getVariableName("<>"));
        Assertions.assertEquals("", VariableNameUtil.getVariableName(""));
    }
}
