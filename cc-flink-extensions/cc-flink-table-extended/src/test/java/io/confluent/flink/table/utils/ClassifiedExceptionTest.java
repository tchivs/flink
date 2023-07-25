/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import io.confluent.flink.table.utils.ClassifiedException.ExceptionClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.confluent.flink.table.service.ServiceTasks.INSTANCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for {@link ClassifiedException}. */
@Confluent
public class ClassifiedExceptionTest {

    private static Stream<TestSpec> testData() {
        return Stream.of(
                // ---
                TestSpec.test("unsupported ANALYZE")
                        .executeSql("ANALYZE TABLE x COMPUTE STATISTICS")
                        .expectUserError("The requested operation is not supported."),
                // ---
                TestSpec.test("better OPTION hint error")
                        .executeSql("CREATE TABLE t (i INT) WITH ('connector' = 'datagen')")
                        .executeSql("SELECT * FROM t /*+ OPTIONS('rows-per-second' = '42') */")
                        .expectUserError(
                                "Cannot accept 'OPTIONS' hint. Please remove it from the query."),
                // ---
                TestSpec.test("caused by chain")
                        .executeSql(
                                "CREATE TABLE t (i INT) WITH ('connector' = 'datagen', 'invalid' = 'option')")
                        .executeSql("SELECT * FROM t")
                        .expectUserError(
                                "Unable to create a source for reading table 'default_catalog.default_database.t'.\n"
                                        + "\n"
                                        + "Table options are:\n"
                                        + "\n"
                                        + "'connector'='datagen'\n"
                                        + "'invalid'='option'\n"
                                        + "\n"
                                        + "Caused by: Unsupported options found for 'datagen'.\n"
                                        + "\n"
                                        + "Unsupported options:\n"
                                        + "\n"
                                        + "invalid\n"
                                        + "\n"
                                        + "Supported options:\n"
                                        + "\n"
                                        + "connector\n"
                                        + "fields.i.kind\n"
                                        + "fields.i.max\n"
                                        + "fields.i.min\n"
                                        + "fields.i.null-rate\n"
                                        + "number-of-rows\n"
                                        + "rows-per-second"),
                // ---
                TestSpec.test("cause by chain but ignore top-level")
                        .executeSql("CREATE TABLE t (i INT)")
                        .expectUserError(
                                "Table options do not contain an option key 'connector' for discovering a connector.")
                        .shouldNotContain("Could not execute CreateTable"),
                // ---
                TestSpec.test("cause by chain but with additional allowed cause")
                        .executeSql(
                                "CREATE TABLE t (i INT) WITH ('connector' = 'datagen', 'number-of-rows' = 'INVALID')")
                        .executeSql("SELECT * FROM t")
                        .allowAdditionalCause(IllegalArgumentException.class)
                        .expectUserError(
                                "Could not parse value 'INVALID' for key 'number-of-rows'."),
                // ---
                TestSpec.test("not showing catalogs")
                        .executeSql("INSERT INTO t SELECT 1;")
                        .expectExactUserError(
                                "Cannot find table '`default_catalog`.`default_database`.`t`'."));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testClassifiedException(TestSpec testSpec) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(tableEnv);
        try {
            testSpec.sqlStatements.forEach(tableEnv::executeSql);
            fail("Exception should have occurred");
        } catch (Exception e) {
            final ClassifiedException classified =
                    ClassifiedException.of(e, testSpec.allowedCauses);
            switch (testSpec.matchCondition) {
                case EXACT:
                    assertThat(classified.getMessage()).isEqualTo(testSpec.expectedError);
                    break;
                case CONTAIN:
                    assertThat(classified.getMessage()).contains(testSpec.expectedError);
                    break;
            }
            if (testSpec.unexpectedError != null) {
                assertThat(classified.getMessage()).doesNotContain(testSpec.unexpectedError);
            }
            assertThat(classified.getExceptionClass()).isEqualTo(testSpec.expectedClass);
        }
    }

    private static class TestSpec {

        final String description;
        final List<String> sqlStatements = new ArrayList<>();
        final Set<Class<? extends Exception>> allowedCauses = new HashSet<>();

        String expectedError;
        ExceptionClass expectedClass;
        @Nullable String unexpectedError;
        MatchCondition matchCondition;

        enum MatchCondition {
            EXACT,
            CONTAIN
        }

        private TestSpec(String description) {
            this.description = description;
            allowedCauses.add(TableException.class);
            allowedCauses.add(ValidationException.class);
            allowedCauses.add(SqlValidateException.class);
        }

        static TestSpec test(String description) {
            return new TestSpec(description);
        }

        TestSpec executeSql(String sql) {
            this.sqlStatements.add(sql);
            return this;
        }

        TestSpec expectUserError(String expectedError) {
            this.expectedError = expectedError;
            this.expectedClass = ExceptionClass.PLANNING_USER;
            this.matchCondition = MatchCondition.CONTAIN;
            return this;
        }

        TestSpec expectExactUserError(String expectedError) {
            this.expectedError = expectedError;
            this.expectedClass = ExceptionClass.PLANNING_USER;
            this.matchCondition = MatchCondition.EXACT;
            return this;
        }

        TestSpec expectSystemError(String expectedError) {
            this.expectedError = expectedError;
            this.expectedClass = ExceptionClass.PLANNING_SYSTEM;
            this.matchCondition = MatchCondition.CONTAIN;
            return this;
        }

        TestSpec shouldNotContain(String unexpectedError) {
            this.unexpectedError = unexpectedError;
            return this;
        }

        TestSpec allowAdditionalCause(Class<? extends Exception> clazz) {
            this.allowedCauses.add(clazz);
            return this;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
