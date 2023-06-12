/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanCompilation;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.delegation.InternalPlan;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;

import org.apache.flink.shaded.guava31.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.connectors.ForegroundResultTableSink;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Default implementation of {@link ServiceTasks}. */
@Confluent
class DefaultServiceTasks implements ServiceTasks {

    // --------------------------------------------------------------------------------------------
    // compileForegroundQuery
    // --------------------------------------------------------------------------------------------

    @Override
    public ForegroundResultPlan compileForegroundQuery(
            TableEnvironment tableEnvironment, QueryOperation queryOperation) {
        final TableEnvironmentImpl tableEnv = (TableEnvironmentImpl) tableEnvironment;

        final SinkModifyOperation modifyOperation = convertToModifyOperation(queryOperation);

        final InternalPlan plan =
                tableEnv.getPlanner().compilePlan(Collections.singletonList(modifyOperation));

        final String operatorId = extractOperatorId(tableEnv.getConfig(), plan);

        return new ForegroundResultPlan(plan.asJsonString(), operatorId);
    }

    /**
     * Wraps the given {@link QueryOperation} into a {@link SinkModifyOperation} referencing the
     * {@link ForegroundResultTableFactory}.
     */
    private static SinkModifyOperation convertToModifyOperation(QueryOperation queryOperation) {
        final ResolvedSchema childSchema = queryOperation.getResolvedSchema();
        final ResolvedSchema schema =
                ResolvedSchema.physical(
                        childSchema.getColumnNames(), childSchema.getColumnDataTypes());

        final TableDescriptor tableDescriptor =
                TableDescriptor.forConnector(ForegroundResultTableFactory.IDENTIFIER)
                        .schema(Schema.newBuilder().fromResolvedSchema(schema).build())
                        .build();

        final ResolvedCatalogTable resolvedTable =
                new ResolvedCatalogTable(tableDescriptor.toCatalogTable(), schema);

        final ContextResolvedTable contextResolvedTable =
                ContextResolvedTable.anonymous(resolvedTable);

        return new SinkModifyOperation(
                contextResolvedTable,
                queryOperation,
                Collections.emptyMap(),
                null,
                false,
                Collections.emptyMap());
    }

    private static final String UID_FORMAT = "<id>_<transformation>";

    private static String extractOperatorId(ReadableConfig config, InternalPlan plan)
            throws IllegalArgumentException {
        // Extract ExecNode
        final ExecNodeGraphInternalPlan internalPlan = (ExecNodeGraphInternalPlan) plan;
        final List<ExecNode<?>> rootNodes = internalPlan.getExecNodeGraph().getRootNodes();
        if (rootNodes.size() != 1 || !(rootNodes.get(0) instanceof CommonExecSink)) {
            throw new IllegalArgumentException("Foreground queries should produce a single sink.");
        }
        final CommonExecSink sinkNode = (CommonExecSink) rootNodes.get(0);

        // Regenerate UID
        if (!config.get(ExecutionConfigOptions.TABLE_EXEC_UID_FORMAT).equals(UID_FORMAT)) {
            throw new IllegalArgumentException("UID format must be: " + UID_FORMAT);
        }
        final String sinkUid =
                StringUtils.replaceEach(
                        UID_FORMAT,
                        new String[] {"<id>", "<transformation>"},
                        new String[] {
                            String.valueOf(sinkNode.getId()),
                            ForegroundResultTableSink.TRANSFORMATION_NAME
                        });

        // Hash UID (must be kept in sync with Flink's StreamGraphHasherV2)
        final Hasher hasher = Hashing.murmur3_128(0).newHasher();
        hasher.putString(sinkUid, StandardCharsets.UTF_8);
        final byte[] hash = hasher.hash().asBytes();

        // Generate OperatorID
        final OperatorID operatorId = new OperatorID(hash);
        return operatorId.toHexString();
    }

    // --------------------------------------------------------------------------------------------
    // compileBackgroundQueries
    // --------------------------------------------------------------------------------------------

    @Override
    public BackgroundResultPlan compileBackgroundQueries(
            TableEnvironment tableEnvironment, List<ModifyOperation> modifyOperations) {
        final CompilationResult compilationResult = compilePlan(tableEnvironment, modifyOperations);
        return new BackgroundResultPlan(
                compilationResult.compiledPlan, compilationResult.connectorMetadata);
    }

    // --------------------------------------------------------------------------------------------
    // Common methods
    // --------------------------------------------------------------------------------------------

    private static class CompilationResult {
        final String compiledPlan;
        final List<ConnectorMetadata> connectorMetadata;

        CompilationResult(String compiledPlan, List<ConnectorMetadata> connectorMetadata) {
            this.compiledPlan = compiledPlan;
            this.connectorMetadata = connectorMetadata;
        }
    }

    private static CompilationResult compilePlan(
            TableEnvironment tableEnvironment, List<ModifyOperation> modifyOperations) {
        final TableEnvironmentImpl tableEnv = (TableEnvironmentImpl) tableEnvironment;

        // Ensure that connector options do not end up in the compiled plan.
        // They will be maintained separately.
        tableEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS,
                        CatalogPlanCompilation.SCHEMA);

        final InternalPlan plan = tableEnv.getPlanner().compilePlan(modifyOperations);

        final ExecNodeGraph graph = ((ExecNodeGraphInternalPlan) plan).getExecNodeGraph();

        final List<ConnectorMetadata> connectorMetadata = new ArrayList<>();
        graph.getRootNodes().forEach(node -> collectConnectorMetadata(connectorMetadata, node));

        return new CompilationResult(plan.asJsonString(), connectorMetadata);
    }

    private static void collectConnectorMetadata(
            List<ConnectorMetadata> connectorMetadata, ExecNode<?> node) {
        node.getInputEdges()
                .forEach(edge -> collectConnectorMetadata(connectorMetadata, edge.getSource()));

        final ContextResolvedTable contextTable;
        if (node instanceof StreamExecTableSourceScan) {
            final StreamExecTableSourceScan scan = (StreamExecTableSourceScan) node;
            contextTable = scan.getTableSourceSpec().getContextResolvedTable();
        } else if (node instanceof StreamExecLookupJoin) {
            final StreamExecLookupJoin lookupJoin = (StreamExecLookupJoin) node;
            contextTable =
                    lookupJoin
                            .getTemporalTableSourceSpec()
                            .getTableSourceSpec()
                            .getContextResolvedTable();
        } else if (node instanceof StreamExecSink) {
            final StreamExecSink sink = (StreamExecSink) node;
            contextTable = sink.getTableSinkSpec().getContextResolvedTable();
        } else {
            return;
        }

        if (!(contextTable.getTable() instanceof ConfluentCatalogTable)) {
            throw new IllegalArgumentException("Confluent managed catalog table expected.");
        }
        final ConfluentCatalogTable catalogTable = contextTable.getTable();

        final Map<String, String> allOptions = new HashMap<>();
        allOptions.putAll(catalogTable.getOptions());
        allOptions.putAll(catalogTable.getPrivateOptions());

        connectorMetadata.add(new ConnectorMetadata(contextTable.getIdentifier(), allOptions));
    }
}
