/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.extraction;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests {@link MetadataExtractor}. */
public class MetadataExtractorTest {

    @Test
    public void testSimple() throws Throwable {
        Metadata metadata =
                MetadataExtractor.extract(
                        getClass().getClassLoader(), FunctionSimple.class.getName());
        assertThat(metadata.getSignatures().size()).isEqualTo(1);
        assertThat(metadata.getSignatures().get(0).getSerializedArgumentTypes())
                .containsExactly("INT NOT NULL");
        assertThat(metadata.getSignatures().get(0).getSerializedReturnType())
                .isEqualTo("VARCHAR(2147483647)");
    }

    @Test
    public void testOverloaded() throws Throwable {
        Metadata metadata =
                MetadataExtractor.extract(
                        getClass().getClassLoader(), FunctionOverloaded.class.getName());
        assertThat(metadata.getSignatures().size()).isEqualTo(2);
        assertThat(metadata.getSignatures().get(0).getSerializedArgumentTypes())
                .containsExactly("INT NOT NULL");
        assertThat(metadata.getSignatures().get(0).getSerializedReturnType())
                .isEqualTo("VARCHAR(2147483647)");
        assertThat(metadata.getSignatures().get(1).getSerializedArgumentTypes())
                .containsExactly("BIGINT", "VARCHAR(2147483647)");
        assertThat(metadata.getSignatures().get(1).getSerializedReturnType()).isEqualTo("INT");
    }

    @Test
    public void testVararg() throws Throwable {
        assertThatThrownBy(
                        () ->
                                MetadataExtractor.extract(
                                        getClass().getClassLoader(),
                                        FunctionVararg.class.getName()))
                .hasMessageContaining("Varargs are not allowed");
    }

    @Test
    public void testCustomTypeInference() throws Throwable {
        assertThatThrownBy(
                        () ->
                                MetadataExtractor.extract(
                                        getClass().getClassLoader(),
                                        CustomTypeInferenceFunction.class.getName()))
                .hasMessageContaining("Custom type inference not supported");
    }

    @Test
    public void testFunctionHints() throws Throwable {
        Metadata metadata =
                MetadataExtractor.extract(
                        getClass().getClassLoader(), FullFunctionHints.class.getName());
        assertThat(metadata.getSignatures().size()).isEqualTo(2);
        assertThat(metadata.getSignatures().get(0).getSerializedArgumentTypes())
                .containsExactly("BIGINT");
        assertThat(metadata.getSignatures().get(0).getSerializedReturnType()).isEqualTo("BIGINT");
        assertThat(metadata.getSignatures().get(1).getSerializedArgumentTypes())
                .containsExactly("INT");
        assertThat(metadata.getSignatures().get(1).getSerializedReturnType()).isEqualTo("INT");
    }

    @Test
    public void testZeroArg() throws Throwable {
        Metadata metadata =
                MetadataExtractor.extract(
                        getClass().getClassLoader(), ZeroArgFunction.class.getName());
        assertThat(metadata.getSignatures().size()).isEqualTo(1);
        assertThat(metadata.getSignatures().get(0).getSerializedArgumentTypes()).containsExactly();
        assertThat(metadata.getSignatures().get(0).getSerializedReturnType()).isEqualTo("INT");
    }

    /** Simple function. */
    public static class FunctionSimple extends ScalarFunction {
        public String eval(int a) {
            return "blah_" + a;
        }
    }

    /** Overloaded function. */
    public static class FunctionOverloaded extends ScalarFunction {
        public String eval(int a) {
            return "blah_" + a;
        }

        public Integer eval(Long a, String b) {
            return a.intValue();
        }
    }

    /** Vararg function. */
    public static class FunctionVararg extends ScalarFunction {
        public String eval(Integer... a) {
            return "blah_" + a.length;
        }
    }

    /** Hint function. */
    @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
    @FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
    public static class FullFunctionHints extends ScalarFunction {
        public Number eval(Number n) {
            return null;
        }
    }

    /** Zero arg function. */
    public static class ZeroArgFunction extends ScalarFunction {
        public Integer eval() {
            return null;
        }
    }

    /** Custom type inference function. */
    public static class CustomTypeInferenceFunction extends ScalarFunction {
        public String eval(int a) {
            return "blah_" + a;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return null;
        }
    }
}
