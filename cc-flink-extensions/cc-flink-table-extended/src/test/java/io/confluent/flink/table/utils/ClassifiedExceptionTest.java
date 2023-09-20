/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;

import io.confluent.flink.table.service.ServiceTasks.Service;
import io.confluent.flink.table.utils.ClassifiedException.ExceptionClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
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
                        .executeSql("INSERT INTO t SELECT 1")
                        .expectExactUserError(
                                "Cannot find table '`default_catalog`.`default_database`.`t`'."),
                // ---
                TestSpec.test("unsupported scripts")
                        .executeSql("SELECT 1; SELECT 1;")
                        .expectExactUserError(
                                "Only a single statement is supported at a time. "
                                        + "Multiple INSERT INTO statements can be wrapped into a STATEMENT SET."),
                // ---
                TestSpec.test("unknown current database")
                        .executeSql("USE bad_db")
                        .expectExactUserError(
                                "A database with name 'bad_db' does not exist, or you "
                                        + "have no permissions to access it in catalog 'default_catalog'."),
                // ---
                TestSpec.test("unknown current catalog")
                        .executeSql("USE CATALOG bad_cat")
                        .expectExactUserError(
                                "A catalog with name 'bad_cat' does not exist, or "
                                        + "you have no permissions to access it."),
                // ---
                TestSpec.test("window group by on non rowtime column")
                        .executeSql(
                                "CREATE TABLE orders(productId BIGINT, amount INT,"
                                        + " orderDate TIMESTAMP_LTZ(3)) WITH ('connector' = 'datagen')")
                        .executeSql(
                                "SELECT productId, SUM(amount), CAST(window_start AS DATE) FROM"
                                        + " TABLE(TUMBLE( TABLE orders, DESCRIPTOR(orderDate),"
                                        + " INTERVAL '24' HOURS))"
                                        + " GROUP BY productId,  window_start, window_end")
                        .expectUserError(
                                "The window function requires the timecol is"
                                        + " a time attribute type, but is"
                                        + " TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)."),
                // ---
                TestSpec.test("hide internal errors during SQL validation phase")
                        .setupTableEnvironment(
                                tableEnv -> tableEnv.registerCatalog("c", new InvalidCatalog("c")))
                        .executeSql("SELECT * FROM c.db.t")
                        .expectSystemError("SQL validation failed. Internal error!"),
                // ---
                TestSpec.test("show helpful error during SQL validation in range")
                        .executeSql("SELECT notExisting()")
                        .expectExactUserError(
                                "SQL validation failed. Error from line 1, column 8 to line 1, column 20.\n"
                                        + "\n"
                                        + "Caused by: No match found for function signature notExisting()"),
                // ---
                TestSpec.test("show helpful error during SQL validation at point")
                        .executeSql("SELECT n")
                        .expectExactUserError(
                                "SQL validation failed. Error at or near line 1, column 8.\n"
                                        + "\n"
                                        + "Caused by: Column 'n' not found in any table"),
                // ---
                TestSpec.test("adding watermark is always modify due to system defaults")
                        .executeSql(
                                "CREATE TABLE t (t TIMESTAMP(3), WATERMARK FOR t AS t) "
                                        + "WITH ('connector' = 'datagen')")
                        .executeSql("ALTER TABLE t ADD WATERMARK FOR t AS t - INTERVAL '1' SECOND")
                        .expectExactUserError(
                                "All tables declare a system-provided watermark by default. "
                                        + "Use ALTER TABLE MODIFY for custom watermarks."));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testClassifiedException(TestSpec testSpec) {
        final TableEnvironment tableEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), Collections.emptyMap(), Service.SQL_SERVICE);
        testSpec.tableEnvSetup.forEach(consumer -> consumer.accept(tableEnv));
        try {
            testSpec.sqlStatements.forEach(tableEnv::executeSql);
            fail("Throwable should have occurred");
        } catch (Throwable t) {
            assertThat(t).isInstanceOf(Exception.class);
            final ClassifiedException classified =
                    ClassifiedException.of((Exception) t, testSpec.allowedCauses);
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
        final List<Consumer<TableEnvironment>> tableEnvSetup = new ArrayList<>();
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
            allowedCauses.addAll(ClassifiedException.VALID_CAUSES);
        }

        static TestSpec test(String description) {
            return new TestSpec(description);
        }

        TestSpec executeSql(String sql) {
            this.sqlStatements.add(sql);
            return this;
        }

        TestSpec setupTableEnvironment(Consumer<TableEnvironment> consumer) {
            this.tableEnvSetup.add(consumer);
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

    // --------------------------------------------------------------------------------------------
    // Supporting classes for creating exceptions
    // --------------------------------------------------------------------------------------------

    /** Exception throwing {@link Catalog}. */
    public static class InvalidCatalog extends GenericInMemoryCatalog {

        public InvalidCatalog(String name) {
            super(name, "db");
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath) {
            throw new RuntimeException("Internal error!");
        }
    }
}
