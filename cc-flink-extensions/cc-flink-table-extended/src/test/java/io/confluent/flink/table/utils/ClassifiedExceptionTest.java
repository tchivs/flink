/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import io.confluent.flink.table.service.ServiceTasks.Service;
import io.confluent.flink.table.utils.ClassifiedException.ExceptionKind;
import io.confluent.flink.table.utils.ClassifiedExceptionTest.InvalidCatalog.ExceptionType;
import org.junit.jupiter.api.Test;
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
                TestSpec.test("remove confluent specific table options")
                        .executeSql(
                                "CREATE TABLE t (i INT) \n"
                                        + "WITH (\n"
                                        + "'changelog.mode'='append',\n"
                                        + "'confluent.kafka.bootstrap-servers'='SASL_SSL://pkc-kod82.us-west-2.aws.stag.cpdev.cloud:9092',\n"
                                        + "'confluent.kafka.consumer-group-id'='0043fab0-253c-49aa-a26d-5a7ea0c5a808_903',\n"
                                        + "'confluent.kafka.logical-cluster-id'='lkc-q9jgvd',\n"
                                        + "'connector'='confluent',\n"
                                        + "'key.format'='avro-registry',\n"
                                        + "'key.avro-registry.confluent.schema-id'='100010',\n"
                                        + "'value.avro-registry.confluent.logical-cluster-id'='lsrc-7z6xd1',\n"
                                        + "'value.avro-registry.confluent.url'='https://psrc-y13pj.us-west-2.aws.stag.cpdev.cloud',\n"
                                        + "'value.format'='avro-registry',\n"
                                        + "'invalid' = 'option')")
                        .executeSql("SELECT * FROM t")
                        .expectUserError(
                                "Unable to create a source for reading table 'default_catalog.default_database.t'.\n"
                                        + "\n"
                                        + "Table options are:\n"
                                        + "\n"
                                        + "'changelog.mode'='append'\n"
                                        + "'connector'='confluent'\n"
                                        + "'invalid'='option'\n"
                                        + "'key.format'='avro-registry'\n"
                                        + "'value.format'='avro-registry'"),
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
                                "A catalog with name or id 'bad_cat' cannot be resolved. Possible reasons:\n"
                                        + "\t1. You might not have permissions to access it.\n"
                                        + "\t2. The catalog might not exist.\n"
                                        + "\t3. There might be multiple catalogs with the same name."),
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
                TestSpec.test("Validation exceptions from ConfluentFlinkCatalog are exposed")
                        .setupTableEnvironment(
                                tableEnv ->
                                        tableEnv.registerCatalog(
                                                "c",
                                                new InvalidCatalog("c", ExceptionType.VALIDATION)))
                        .executeSql(
                                String.format(
                                        "SELECT * FROM c.`%s`.`%s`", InvalidCatalog.DB_NAME, "t"))
                        .expectUserError("Validation exception"),

                // ---
                TestSpec.test("Table exceptions from ConfluentFlinkCatalog are exposed")
                        .setupTableEnvironment(
                                tableEnv ->
                                        tableEnv.registerCatalog(
                                                "c", new InvalidCatalog("c", ExceptionType.TABLE)))
                        .executeSql(
                                String.format(
                                        "SELECT * FROM c.`%s`.`%s`", InvalidCatalog.DB_NAME, "t"))
                        .expectUserError("Table exception"),

                // ---
                TestSpec.test("Catalog exceptions from ConfluentFlinkCatalog are NOT exposed")
                        .setupTableEnvironment(
                                tableEnv ->
                                        tableEnv.registerCatalog(
                                                "c",
                                                new InvalidCatalog("c", ExceptionType.CATALOG)))
                        .executeSql(
                                String.format(
                                        "SELECT * FROM c.`%s`.`%s`", InvalidCatalog.DB_NAME, "t"))
                        .expectSystemError("Internal error!"),

                // ---
                TestSpec.test("show helpful error during SQL validation in range")
                        .executeSql("SELECT notExisting()")
                        .expectExactUserError(
                                "SQL validation failed. Error from line 1, column 8 to line 1, column 20.\n"
                                        + "\n"
                                        + "Caused by: No match found for function signature notExisting()"),
                // ---
                TestSpec.test("type inference failed during validation")
                        .executeSql("CREATE TABLE t (a BIGINT) " + "WITH ('connector' = 'datagen')")
                        .executeSql("SELECT ARRAY_JOIN() FROM t")
                        .expectExactUserError(
                                "SQL validation failed. Error from line 1, column 8 to line 1, column 19.\n"
                                        + "\n"
                                        + "Caused by: No match found for function signature ARRAY_JOIN()"),
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
                                        + "Use ALTER TABLE MODIFY for custom watermarks."),
                // ---
                TestSpec.test("select a table that does not exist")
                        .executeSql("SELECT * FROM `not_exist`")
                        .expectExactUserError(
                                "SQL validation failed. Error from line 1, column 15 to line 1, column 25.\n"
                                        + "\n"
                                        + "Caused by: Table (or view) 'not_exist' does not exist or you do not have permission to access it.\n"
                                        + "Using current catalog '' and current database ''."),

                // ---
                TestSpec.test("alter a table when no database is set")
                        .setupTableEnvironment(env -> env.useDatabase(null))
                        .executeSql("ALTER TABLE t ADD (b INT)")
                        .expectExactUserError(
                                "A current, valid database has not been set. Please use a fully qualified identifier (such as 'my_database.my_table' or 'my_catalog.my_database.my_table') or set a current database using 'USE my_database'."),

                // ---
                TestSpec.test("alter a table when no catalog is set")
                        .setupTableEnvironment(env -> env.useCatalog(null))
                        .executeSql("ALTER TABLE t ADD (b INT)")
                        .expectExactUserError(
                                "A current, valid catalog has not been set. Please use a fully qualified identifier (such as 'my_catalog.my_database.my_table') or set a current catalog using 'USE CATALOG my_catalog'."),

                // ---
                TestSpec.test("removal of sensitive string literals")
                        .executeSql("SELECT * FROM `not_exist`")
                        .expectLogError(
                                ExceptionKind.USER,
                                "SQL validation failed. Error from line 1, column 15 to line 1, column 25.\n"
                                        + "\n"
                                        + "Caused by: Table (or view) '<<removed>>' does not exist or you do not have permission to access it."),
                // ---
                TestSpec.test("error for unsupported RAW type from CREATE TABLE")
                        .executeSql(
                                "CREATE TABLE t (r RAW('java.lang.Object', 'xyz')) "
                                        + "WITH ('connector' = 'datagen')")
                        .expectExactUserError(
                                "The use of RAW types is not supported. "
                                        + "Use standard SQL types to represent 'java.lang.Object' objects. "
                                        + "Or use BYTES and implement custom serialization logic."),
                // ---
                TestSpec.test("error for unsupported RAW type from SELECT")
                        .executeSql("SELECT CAST('1' AS RAW('java.lang.Object', 'xyz'))")
                        .expectExactUserError(
                                "The use of RAW types is not supported. "
                                        + "Use standard SQL types to represent 'java.lang.Object' objects. "
                                        + "Or use BYTES and implement custom serialization logic."));
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
                    ClassifiedException.of(
                            (Exception) t, testSpec.allowedCauses, new Configuration());
            switch (testSpec.matchCondition) {
                case SENSITIVE_MESSAGE_EXACT:
                    assertThat(classified.getSensitiveMessage()).isEqualTo(testSpec.expectedError);
                    break;
                case SENSITIVE_MESSAGE_CONTAIN:
                    assertThat(classified.getSensitiveMessage()).contains(testSpec.expectedError);
                    break;
                case LOG_MESSAGE_CONTAIN:
                    assertThat(classified.getLogMessage()).contains(testSpec.expectedError);
                    break;
            }
            if (testSpec.unexpectedError != null) {
                assertThat(classified.getSensitiveMessage())
                        .doesNotContain(testSpec.unexpectedError);
            }
            assertThat(classified.getKind()).isEqualTo(testSpec.expectedKind);
        }
    }

    @Test
    void testLogException() {
        final ValidationException e =
                new ValidationException(
                        "Error with 'sensitive' field of 'Bob''s message'.",
                        new TableException("Error in table '' or 'Alice''s or Bob''s table'."));
        final ClassifiedException classified =
                ClassifiedException.of(e, ClassifiedException.VALID_CAUSES, new Configuration());
        assertThat(classified.getKind()).isEqualTo(ExceptionKind.USER);
        assertThat(classified.getLogMessage())
                .isEqualTo(
                        "Error with '<<removed>>' field of '<<removed>>'.\n\n"
                                + "Caused by: Error in table '<<removed>>' or '<<removed>>'.");

        final Exception cleanedException = classified.getLogException();
        assertThat(cleanedException.getMessage())
                .isEqualTo(
                        "org.apache.flink.table.api.ValidationException: Error with '<<removed>>' field of '<<removed>>'.");
        assertThat(cleanedException.getCause().getMessage())
                .isEqualTo(
                        "org.apache.flink.table.api.TableException: Error in table '<<removed>>' or '<<removed>>'.");
        assertThat(cleanedException.getStackTrace()).isEqualTo(e.getStackTrace());
    }

    private static class TestSpec {

        final String description;
        final List<Consumer<TableEnvironment>> tableEnvSetup = new ArrayList<>();
        final List<String> sqlStatements = new ArrayList<>();
        final Set<Class<? extends Throwable>> allowedCauses = new HashSet<>();

        String expectedError;
        ExceptionKind expectedKind;
        @Nullable String unexpectedError;
        MatchCondition matchCondition;

        enum MatchCondition {
            SENSITIVE_MESSAGE_EXACT,
            SENSITIVE_MESSAGE_CONTAIN,
            LOG_MESSAGE_CONTAIN
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
            this.expectedKind = ExceptionKind.USER;
            this.matchCondition = MatchCondition.SENSITIVE_MESSAGE_CONTAIN;
            return this;
        }

        TestSpec expectExactUserError(String expectedError) {
            this.expectedError = expectedError;
            this.expectedKind = ExceptionKind.USER;
            this.matchCondition = MatchCondition.SENSITIVE_MESSAGE_EXACT;
            return this;
        }

        TestSpec expectSystemError(String expectedError) {
            this.expectedError = expectedError;
            this.expectedKind = ExceptionKind.INTERNAL;
            this.matchCondition = MatchCondition.SENSITIVE_MESSAGE_CONTAIN;
            return this;
        }

        TestSpec expectLogError(ExceptionKind expectedKind, String expectedError) {
            this.expectedError = expectedError;
            this.expectedKind = expectedKind;
            this.matchCondition = MatchCondition.LOG_MESSAGE_CONTAIN;
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

        public static final String DB_NAME = "db";
        private final ExceptionType exceptionType;

        /** Defines what kind of exception should be thrown by the catalog. */
        public enum ExceptionType {
            VALIDATION,
            TABLE,
            CATALOG
        }

        public InvalidCatalog(String name) {
            this(name, ExceptionType.CATALOG);
        }

        public InvalidCatalog(String name, ExceptionType exceptionType) {
            super(name, DB_NAME);
            this.exceptionType = exceptionType;
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath) {
            throw getException();
        }

        private RuntimeException getException() {
            switch (exceptionType) {
                case VALIDATION:
                    return new ValidationException("Validation exception");
                case TABLE:
                    return new TableException("Table exception");
                case CATALOG:
                    return new CatalogException("Internal error!");
                default:
                    throw new IllegalStateException("Unknown exception type.");
            }
        }
    }
}
