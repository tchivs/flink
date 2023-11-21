/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.local;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MiniEvaluator}. */
@Confluent
public class MiniEvaluatorTest {

    private static TableEnvironmentImpl tableEnv;

    @BeforeAll
    static void setUp() {
        tableEnv =
                (TableEnvironmentImpl)
                        TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnv.executeSql("CREATE TABLE sink (s STRING, i INT) WITH ('connector' = 'blackhole')");
        tableEnv.executeSql(
                "CREATE TABLE source1 (name STRING, score INT, color STRING) WITH ('connector' = 'datagen')");
        tableEnv.executeSql(
                "CREATE TABLE source2 (name STRING, score INT, color STRING) WITH ('connector' = 'datagen')");
    }

    @Test
    void testCalcWithoutCondition() {
        assertResult(
                "SELECT name, score FROM source1",
                GenericRowData.of(StringData.fromString("Tom"), 42),
                GenericRowData.of(StringData.fromString("Alice"), 100),
                GenericRowData.of(StringData.fromString("Bob"), null));
    }

    @Test
    void testCalcWithCondition() {
        assertResult(
                "SELECT name, score FROM source1 WHERE name = 'Tom'",
                GenericRowData.of(StringData.fromString("Tom"), 42));
    }

    @Test
    void testCalcWithSearchCondition() {
        assertResult(
                "SELECT name, score FROM source1 WHERE name = 'Tom' OR name = 'Bob'",
                GenericRowData.of(StringData.fromString("Tom"), 42),
                GenericRowData.of(StringData.fromString("Bob"), null));
    }

    @Test
    void testUnion() {
        assertResult(
                "(SELECT name, score FROM source1 WHERE name = 'Bob') "
                        + "UNION ALL (SELECT name, score FROM source2)",
                GenericRowData.of(StringData.fromString("Bob"), null),
                GenericRowData.of(StringData.fromString("Tom"), 42),
                GenericRowData.of(StringData.fromString("Alice"), 100),
                GenericRowData.of(StringData.fromString("Bob"), null));
    }

    @Test
    void testStringLiteral() {
        assertResult(
                "SELECT 'Name', score FROM source1",
                GenericRowData.of(StringData.fromString("Name"), 42),
                GenericRowData.of(StringData.fromString("Name"), 100),
                GenericRowData.of(StringData.fromString("Name"), null));
    }

    @Test
    void testIntLiteral() {
        assertResult(
                "SELECT name, 300 FROM source1",
                GenericRowData.of(StringData.fromString("Tom"), 300),
                GenericRowData.of(StringData.fromString("Alice"), 300),
                GenericRowData.of(StringData.fromString("Bob"), 300));
    }

    @Test
    void testNullLiteral() {
        assertResult(
                "SELECT CAST(NULL AS STRING), CAST(NULL AS INT) FROM source1 WHERE name = 'Bob'",
                GenericRowData.of(null, null));
    }

    @Test
    void testEquals() {
        assertResult(
                "SELECT name, score FROM source1 WHERE name = 'Tom' AND score = 42",
                GenericRowData.of(StringData.fromString("Tom"), 42));
    }

    @Test
    void testNotEquals() {
        assertResult(
                "SELECT name, score FROM source1 WHERE name <> 'Tom' AND score <> 42",
                GenericRowData.of(StringData.fromString("Alice"), 100));
    }

    @Test
    void testIsNull() {
        assertResult(
                "SELECT name, score FROM source1 WHERE score IS NULL",
                GenericRowData.of(StringData.fromString("Bob"), null));
    }

    @Test
    void testIsNotNull() {
        assertResult(
                "SELECT name, score FROM source1 WHERE score IS NOT NULL",
                GenericRowData.of(StringData.fromString("Tom"), 42),
                GenericRowData.of(StringData.fromString("Alice"), 100));
    }

    @Test
    void testOr() {
        assertResult(
                "SELECT name, score FROM source1 WHERE score = '200' OR name = 'Bob'",
                GenericRowData.of(StringData.fromString("Bob"), null));
    }

    @Test
    void testAnd() {
        assertResult("SELECT name, score FROM source1 WHERE score = 200 AND name = 'Bob'");
    }

    @Test
    void testCast() {
        assertResult(
                "SELECT CAST(score AS STRING), CAST(NULL AS INT) FROM source1",
                GenericRowData.of(StringData.fromString("42"), null),
                GenericRowData.of(StringData.fromString("100"), null),
                GenericRowData.of(null, null));
    }

    @Test
    void testUnsupportedNode() {
        assertUnsupported("SELECT 'Martin', 1", "Unsupported node");
    }

    @Test
    void testUnsupportedExpression() {
        assertUnsupported("SELECT UPPER(name), score FROM source1", "Unsupported expression");
    }

    @Test
    void testUnsupportedCast() {
        assertUnsupported(
                "SELECT CAST(name AS VARCHAR(100)), score FROM source1", "Unsupported cast type");
    }

    private static class TestMiniEvaluator extends MiniEvaluator {

        protected TestMiniEvaluator(RexBuilder rexBuilder) {
            super(rexBuilder);
        }

        @Override
        protected Stream<GenericRowData> translateScan(StreamPhysicalTableSourceScan scan) {
            return Stream.of(
                    GenericRowData.of(
                            StringData.fromString("Tom"), 42, StringData.fromString("blue")),
                    GenericRowData.of(
                            StringData.fromString("Alice"), 100, StringData.fromString("green")),
                    GenericRowData.of(StringData.fromString("Bob"), null, null));
        }
    }

    private static void assertResult(String selectSql, GenericRowData... expectedData) {
        final Stream<GenericRowData> stream = toStream(selectSql);
        assertThat(stream.collect(Collectors.toList())).containsExactly(expectedData);
    }

    private static void assertUnsupported(String selectSql, String errorMessage) {
        assertThatThrownBy(() -> toStream(selectSql)).hasMessageContaining(errorMessage);
    }

    private static Stream<GenericRowData> toStream(String selectSql) {
        final StreamPlanner planner = (StreamPlanner) tableEnv.getPlanner();
        final RelNode relNodeWithSink = toPhysicalRel(planner, "INSERT INTO sink " + selectSql);
        final RelNode relNode = relNodeWithSink.getInput(0);

        return new TestMiniEvaluator(planner.createSerdeContext().getRexBuilder())
                .translate(relNode);
    }

    private static RelNode toPhysicalRel(StreamPlanner planner, String sql) {
        final List<Operation> operations = planner.getParser().parse(sql);
        final SinkModifyOperation modifyOperation = (SinkModifyOperation) operations.get(0);
        final RelNode logicalNode = planner.translateToRel(modifyOperation);
        return planner.optimize(logicalNode);
    }
}
