/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.local;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;

import io.confluent.flink.table.connectors.InfoSchemaTableFactory;
import io.confluent.flink.table.connectors.InfoSchemaTableSource;
import io.confluent.flink.table.infoschema.InfoSchemaTables;
import io.confluent.flink.table.service.summary.ExpressionKind;
import io.confluent.flink.table.service.summary.NodeKind;
import io.confluent.flink.table.service.summary.NodeTag;
import io.confluent.flink.table.service.summary.QueryProperty;
import io.confluent.flink.table.service.summary.QuerySummary;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@link LocalExecution} for {@link InfoSchemaTables}. */
@Confluent
public class InfoSchemaExecution implements LocalExecution {

    public static final InfoSchemaExecution INSTANCE = new InfoSchemaExecution();

    private static final Set<NodeKind> SUPPORTED_NODES =
            Stream.concat(MiniEvaluator.SUPPORTED_NODES.stream(), Stream.of(NodeKind.SINK))
                    .collect(Collectors.toSet());

    private static final Set<ExpressionKind> SUPPORTED_EXPRESSIONS =
            MiniEvaluator.SUPPORTED_EXPRESSIONS;

    @Override
    public boolean matches(QuerySummary summary) {
        final List<String> sources =
                summary.getNodeTagValues(NodeTag.SOURCE_CONNECTOR, String.class);
        // Check that at least one INFORMATION_SCHEMA table is accessed
        if (!sources.contains(InfoSchemaTableFactory.IDENTIFIER)) {
            return false;
        }

        // Limit to foreground queries
        if (!summary.hasProperty(QueryProperty.FOREGROUND)) {
            throw errorMessage("Only foreground statements are supported.");
        }

        // Limit to only INFORMATION_SCHEMA tables
        if (sources.stream().anyMatch(s -> !s.equals(InfoSchemaTableFactory.IDENTIFIER))) {
            throw errorMessage("INFORMATION_SCHEMA queries must not access regular tables.");
        }

        // Limit nodes and expressions
        if (summary.getNodeKinds().stream().anyMatch(k -> !SUPPORTED_NODES.contains(k))) {
            throw errorMessage(
                    "Only basic SQL queries (such as SELECT, WHERE, and UNION ALL) are supported.");
        }
        if (summary.getExpressionKinds().stream()
                .anyMatch(k -> !SUPPORTED_EXPRESSIONS.contains(k))) {
            throw errorMessage(
                    "Only basic SQL expressions (such as <>, =, AND, OR, IS NULL) are supported.");
        }

        return true;
    }

    @Override
    public Optional<Stream<RowData>> execute(
            SerdeContext context, List<FlinkPhysicalRel> physicalGraph) {
        // matches() ensures that we can safely strip the sink
        final RelNode body = physicalGraph.get(0).getInput(0);

        final InfoSchemaEvaluator evaluator = new InfoSchemaEvaluator(context);
        try {
            return Optional.of(evaluator.translate(body).map(RowData.class::cast));
        } catch (UnsupportedOperationException e) {
            // Once we support job execution this can return Optional.empty()
            throw errorMessage(e.getMessage());
        }
    }

    private static class InfoSchemaEvaluator extends MiniEvaluator {

        private final SerdeContext context;

        protected InfoSchemaEvaluator(SerdeContext context) {
            super(context.getRexBuilder());
            this.context = context;
        }

        @Override
        protected Stream<GenericRowData> translateScan(StreamPhysicalTableSourceScan scan) {
            final InfoSchemaTableSource infoSchemaSource =
                    (InfoSchemaTableSource) scan.tableSource();
            final String tableName = infoSchemaSource.getTableName();
            final Map<String, String> idColumns = infoSchemaSource.getPushedCatalogIds();

            final Set<String> requiredIdColumns = InfoSchemaTables.getCatalogIdColumns(tableName);
            if (!requiredIdColumns.equals(idColumns.keySet())) {
                throw new TableException(
                        String.format(
                                "Table '%s' cannot be accessed without providing the required columns: %s",
                                scan.tableSourceTable().contextResolvedTable().getIdentifier(),
                                requiredIdColumns));
            }

            return InfoSchemaTables.getStreamForTable(tableName, context, idColumns);
        }
    }

    private static TableException errorMessage(String reason) {
        return new TableException("Access to INFORMATION_SCHEMA is currently limited. " + reason);
    }
}
