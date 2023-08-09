/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.RowtimeInserter;
import org.apache.flink.table.api.config.OptimizerConfigOptions.NonDeterministicUpdateStrategy;
import org.apache.flink.table.api.internal.TableConfigValidation;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;

import org.apache.flink.shaded.guava31.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.configuration.ConnectorOptionsProvider;
import io.confluent.flink.table.configuration.ServiceTaskOptions;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.connectors.ForegroundResultTableSink;
import io.confluent.flink.table.functions.scalar.ai.AIResponseGenerator;
import io.confluent.flink.table.functions.scalar.ai.AISecret;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.configuration.ConfigurationUtils.canBePrefixMap;
import static org.apache.flink.configuration.ConfigurationUtils.filterPrefixMapKey;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.IDLE_STATE_RETENTION;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_ROWTIME_INSERTER;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SOURCE_IDLE_TIMEOUT;
import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY;
import static org.apache.flink.table.api.config.TableConfigOptions.LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED;

/** Default implementation of {@link ServiceTasks}. */
@Confluent
class DefaultServiceTasks implements ServiceTasks {

    // --------------------------------------------------------------------------------------------
    // configureEnvironment
    // --------------------------------------------------------------------------------------------

    @Override
    public void configureEnvironment(
            TableEnvironment tableEnvironment,
            Map<String, String> options,
            boolean performValidation) {
        final Configuration providedOptions = Configuration.fromMap(options);
        if (performValidation) {
            validateConfiguration(providedOptions);
        }

        providedOptions
                .getOptional(ServiceTasksOptions.SQL_CURRENT_CATALOG)
                .ifPresent(tableEnvironment::useCatalog);

        providedOptions
                .getOptional(ServiceTasksOptions.SQL_CURRENT_DATABASE)
                .ifPresent(tableEnvironment::useDatabase);

        final TableConfig config = tableEnvironment.getConfig();

        providedOptions
                .getOptional(ServiceTasksOptions.SQL_STATE_TTL)
                .ifPresent(v -> config.set(IDLE_STATE_RETENTION, v));

        // Compared to Flink, we use UTC as the default. The time zone should not depend
        // on the local system's configuration.
        config.set(
                LOCAL_TIME_ZONE,
                providedOptions.getOptional(ServiceTasksOptions.SQL_LOCAL_TIME_ZONE).orElse("UTC"));

        providedOptions
                .getOptional(ServiceTasksOptions.SQL_TABLES_SCAN_IDLE_TIMEOUT)
                .ifPresent(v -> config.set(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, v));

        // Prevents invalid retractions e.g. through non-deterministic time functions like NOW()
        config.set(
                TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
                NonDeterministicUpdateStrategy.TRY_RESOLVE);

        // Disable OPTION hints
        config.set(TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, false);

        // The limitation of having just a single rowtime attribute column in the query schema
        // causes confusion. The Kafka sink does not use StreamRecord's timestamps anyway.
        config.set(TABLE_EXEC_SINK_ROWTIME_INSERTER, RowtimeInserter.DISABLED);

        // Note: Make sure to set default properties before this line is applied in order to
        // allow DevOps overwriting defaults via JSS if necessary.
        config.addConfiguration(providedOptions);

        // Confluent AI Functions loaded when flag is set
        if (config.get(ServiceTaskOptions.USE_CONFLUENT_AI_FUNCTIONS)) {
            tableEnvironment.createTemporarySystemFunction(
                    "ai_generate", AIResponseGenerator.class);
            tableEnvironment.createTemporarySystemFunction("secret", AISecret.class);
        }
    }

    private static void validateConfiguration(Configuration providedOptions) {
        // FactoryUtil is actually intended for factories but has very convenient
        // validation capabilities. We can replace this call with something custom
        // if necessary.
        FactoryUtil.validateFactoryOptions(
                Collections.emptySet(), ServiceTasksOptions.PUBLIC_OPTIONS, providedOptions);

        try {
            TableConfigValidation.validateTimeZone(
                    providedOptions.get(ServiceTasksOptions.SQL_LOCAL_TIME_ZONE));
        } catch (ValidationException e) {
            throw new ValidationException(
                    String.format(
                            "Invalid value for option '%s'.",
                            ServiceTasksOptions.SQL_LOCAL_TIME_ZONE.key()),
                    e);
        }

        // Also the validation of unconsumed keys is borrowed from FactoryUtil.
        validateUnconsumedKeys(ServiceTasksOptions.PUBLIC_OPTIONS, providedOptions);
    }

    private static void validateUnconsumedKeys(
            Set<ConfigOption<?>> optionalOptions, Configuration providedOptions) {
        final Set<String> consumedOptionKeys =
                optionalOptions.stream()
                        .flatMap(
                                option ->
                                        allKeysExpanded(option, providedOptions.keySet()).stream())
                        .collect(Collectors.toSet());

        final Set<String> deprecatedOptionKeys =
                optionalOptions.stream()
                        .flatMap(DefaultServiceTasks::deprecatedKeys)
                        .collect(Collectors.toSet());

        final Set<String> remainingOptionKeys = new HashSet<>(providedOptions.keySet());

        // Remove consumed keys
        remainingOptionKeys.removeAll(consumedOptionKeys);

        // Remove placeholder keys
        optionalOptions.stream()
                .map(ConfigOption::key)
                .filter(k -> k.endsWith(FactoryUtil.PLACEHOLDER_SYMBOL))
                .map(k -> k.substring(0, k.length() - 1))
                .forEach(
                        prefix ->
                                providedOptions
                                        .keySet()
                                        .forEach(
                                                k -> {
                                                    if (k.startsWith(prefix)) {
                                                        remainingOptionKeys.remove(k);
                                                    }
                                                }));

        if (!remainingOptionKeys.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "Unsupported configuration options found.\n\n"
                                    + "Unsupported options:\n"
                                    + "%s\n\n"
                                    + "Supported options:\n"
                                    + "%s",
                            remainingOptionKeys.stream().sorted().collect(Collectors.joining("\n")),
                            consumedOptionKeys.stream()
                                    // Deprecated keys are not shown to not advertise them.
                                    .filter(k -> !deprecatedOptionKeys.contains(k))
                                    .sorted()
                                    .collect(Collectors.joining("\n"))));
        }
    }

    private static Set<String> allKeysExpanded(ConfigOption<?> option, Set<String> actualKeys) {
        final Set<String> staticKeys = allKeys(option).collect(Collectors.toSet());
        if (!canBePrefixMap(option)) {
            return staticKeys;
        }
        // include all prefix keys of a map option by considering the actually provided keys
        return Stream.concat(
                        staticKeys.stream(),
                        staticKeys.stream()
                                .flatMap(
                                        k ->
                                                actualKeys.stream()
                                                        .filter(c -> filterPrefixMapKey(k, c))))
                .collect(Collectors.toSet());
    }

    private static Stream<String> allKeys(ConfigOption<?> option) {
        return Stream.concat(Stream.of(option.key()), fallbackKeys(option));
    }

    private static Stream<String> fallbackKeys(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .map(FallbackKey::getKey);
    }

    private static Stream<String> deprecatedKeys(ConfigOption<?> option) {
        return StreamSupport.stream(option.fallbackKeys().spliterator(), false)
                .filter(FallbackKey::isDeprecated)
                .map(FallbackKey::getKey);
    }

    // --------------------------------------------------------------------------------------------
    // compileForegroundQuery
    // --------------------------------------------------------------------------------------------

    @Override
    public ForegroundResultPlan compileForegroundQuery(
            TableEnvironment tableEnvironment,
            QueryOperation queryOperation,
            ConnectorOptionsProvider connectorOptions) {
        final SinkModifyOperation modifyOperation = convertToModifyOperation(queryOperation);

        final CompilationResult compilationResult =
                compilePlan(
                        tableEnvironment,
                        Collections.singletonList(modifyOperation),
                        connectorOptions);

        final String operatorId =
                extractOperatorId(tableEnvironment.getConfig(), compilationResult.execNodeGraph);

        return new ForegroundResultPlan(compilationResult.compiledPlan, operatorId);
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

    private static String extractOperatorId(ReadableConfig config, ExecNodeGraph graph)
            throws IllegalArgumentException {
        // Extract ExecNode
        final List<ExecNode<?>> rootNodes = graph.getRootNodes();
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
            TableEnvironment tableEnvironment,
            List<ModifyOperation> modifyOperations,
            ConnectorOptionsProvider connectorOptions) {
        final CompilationResult compilationResult =
                compilePlan(tableEnvironment, modifyOperations, connectorOptions);

        return new BackgroundResultPlan(compilationResult.compiledPlan);
    }

    // --------------------------------------------------------------------------------------------
    // Common methods
    // --------------------------------------------------------------------------------------------

    private static class CompilationResult {
        final ExecNodeGraph execNodeGraph;
        final String compiledPlan;

        CompilationResult(ExecNodeGraph execNodeGraph, String compiledPlan) {
            this.execNodeGraph = execNodeGraph;
            this.compiledPlan = compiledPlan;
        }
    }

    private static CompilationResult compilePlan(
            TableEnvironment tableEnvironment,
            List<ModifyOperation> modifyOperations,
            ConnectorOptionsProvider connectorOptions) {
        final TableEnvironmentImpl tableEnv = (TableEnvironmentImpl) tableEnvironment;

        final StreamPlanner planner = (StreamPlanner) tableEnv.getPlanner();
        final ExecNodeGraphInternalPlan internalPlan =
                (ExecNodeGraphInternalPlan) tableEnv.getPlanner().compilePlan(modifyOperations);
        final ExecNodeGraph graph = internalPlan.getExecNodeGraph();

        graph.getRootNodes().forEach(node -> exposePrivateConnectorOptions(node, connectorOptions));

        graph.getRootNodes().forEach(DefaultServiceTasks::checkForUnsupportedExecNodes);

        final String compiledPlan;
        try {
            compiledPlan =
                    JsonSerdeUtil.createObjectWriter(planner.createSerdeContext())
                            .withDefaultPrettyPrinter()
                            .writeValueAsString(graph);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize given ExecNodeGraph", e);
        }

        return new CompilationResult(graph, compiledPlan);
    }

    private static void exposePrivateConnectorOptions(
            ExecNode<?> node, ConnectorOptionsProvider connectorOptions) {
        node.getInputEdges()
                .forEach(edge -> exposePrivateConnectorOptions(edge.getSource(), connectorOptions));

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

        // This excludes generated connectors like the foreground sink
        if (contextTable.isAnonymous()) {
            return;
        }

        if (!(contextTable.getTable() instanceof ConfluentCatalogTable)) {
            throw new IllegalArgumentException("Confluent managed catalog table expected.");
        }
        final ConfluentCatalogTable catalogTable = contextTable.getTable();

        final Map<String, String> morePrivateOptions =
                connectorOptions.generateOptions(contextTable.getIdentifier(), node.getId());
        catalogTable.exposePrivateOptions(morePrivateOptions);
    }

    private static void checkForUnsupportedExecNodes(ExecNode<?> node) {
        node.getInputEdges().forEach(edge -> checkForUnsupportedExecNodes(edge.getSource()));

        if (node instanceof StreamExecGroupWindowAggregate) {
            throw new TableException(
                    "SQL syntax that calls TUMBLE, HOP, and SESSION in the GROUP BY clause is "
                            + "not supported. Use table-valued function (TVF) syntax instead "
                            + "which is standard compliant.");
        }
    }
}
