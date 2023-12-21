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
import org.apache.flink.table.api.config.TableConfigOptions.CatalogPlanRestore;
import org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy;
import org.apache.flink.table.api.internal.TableConfigValidation;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.SinkModifyOperation;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.serde.SerdeContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;

import org.apache.flink.shaded.guava31.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.connectors.ForegroundResultTableFactory;
import io.confluent.flink.table.connectors.ForegroundResultTableSink;
import io.confluent.flink.table.modules.ai.AIFunctionsModule;
import io.confluent.flink.table.modules.core.CoreProxyModule;
import io.confluent.flink.table.modules.otlp.OtlpFunctionsModule;
import io.confluent.flink.table.modules.remoteudf.RemoteUdfModule;
import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundJobResultPlan;
import io.confluent.flink.table.service.ForegroundResultPlan.ForegroundLocalResultPlan;
import io.confluent.flink.table.service.local.LocalExecution;
import io.confluent.flink.table.service.summary.QuerySummary;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import static org.apache.flink.table.api.config.TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_COLUMN_EXPANSION_STRATEGY;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED;
import static org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toJava;
import static org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala;

/** Default implementation of {@link ServiceTasks}. */
@Confluent
class DefaultServiceTasks implements ServiceTasks {

    // --------------------------------------------------------------------------------------------
    // configureEnvironment
    // --------------------------------------------------------------------------------------------

    private static final String UNKNOWN = "<UNKNOWN>";

    @Override
    public Map<String, String> configureEnvironment(
            TableEnvironment tableEnvironment,
            Map<String, String> publicOptions,
            Map<String, String> privateOptions,
            Service service) {
        final Configuration publicConfig = new Configuration();

        // Ignored keys are removed to avoid conflicts with Flink options (e.g. for "client.")
        publicOptions.entrySet().stream()
                .filter(e -> !ServiceTasksOptions.isIgnored(e.getKey()))
                .forEach(e -> publicConfig.setString(e.getKey(), e.getValue()));

        // Validate options
        if (service == Service.SQL_SERVICE) {
            validateConfiguration(publicConfig);
        }
        applyPublicConfig(tableEnvironment, publicConfig, service);

        final Configuration privateConfig = Configuration.fromMap(privateOptions);
        applyPrivateConfig(tableEnvironment, privateConfig, service);

        tableEnvironment.getConfig().addConfiguration(publicConfig);
        tableEnvironment.getConfig().addConfiguration(privateConfig);

        // Prepare options for persisting in resources
        final Map<String, String> resourceOptions = new HashMap<>(privateConfig.toMap());
        publicConfig
                .toMap()
                .forEach(
                        (k, v) ->
                                resourceOptions.put(
                                        ServiceTasksOptions.PRIVATE_USER_PREFIX + k, v));
        return resourceOptions;
    }

    private void applyPublicConfig(
            TableEnvironment tableEnvironment, Configuration publicConfig, Service service) {
        final TableConfig config = tableEnvironment.getConfig();

        if (service == Service.JOB_SUBMISSION_SERVICE
                && publicConfig.get(ServiceTasksOptions.SQL_DRY_RUN)) {
            throw new IllegalStateException(
                    "Statement submitted for a dry run. It should never reach the JSS.");
        }

        // Handle catalog and database
        if (service == Service.SQL_SERVICE) {
            // Metastore is available
            // "<UNKNOWN>" is a reserved string in ObjectIdentifier and is used for creating an
            // invalid built-in catalog that cannot be accessed.
            publicConfig
                    .getOptional(ServiceTasksOptions.SQL_CURRENT_CATALOG)
                    .filter(v -> !v.isEmpty())
                    .ifPresent(
                            v -> {
                                if (v.equals(UNKNOWN)) {
                                    throw new ValidationException(
                                            String.format("Catalog name '%s' is not allowed.", v));
                                }
                                tableEnvironment.useCatalog(v);
                            });
            publicConfig
                    .getOptional(ServiceTasksOptions.SQL_CURRENT_DATABASE)
                    .filter(v -> !v.isEmpty())
                    .ifPresent(
                            v -> {
                                if (v.equals(UNKNOWN)) {
                                    throw new ValidationException(
                                            String.format("Database name '%s' is not allowed.", v));
                                }
                                tableEnvironment.useDatabase(v);
                            });
        }

        publicConfig
                .getOptional(ServiceTasksOptions.SQL_STATE_TTL)
                .ifPresent(v -> config.set(IDLE_STATE_RETENTION, v));

        // Compared to Flink, we use UTC as the default. The time zone should not depend
        // on the local system's configuration.
        config.set(
                LOCAL_TIME_ZONE,
                publicConfig.getOptional(ServiceTasksOptions.SQL_LOCAL_TIME_ZONE).orElse("UTC"));

        publicConfig
                .getOptional(ServiceTasksOptions.SQL_TABLES_SCAN_IDLE_TIMEOUT)
                .ifPresent(v -> config.set(TABLE_EXEC_SOURCE_IDLE_TIMEOUT, v));
    }

    private void applyPrivateConfig(
            TableEnvironment tableEnvironment, Configuration privateConfig, Service service) {
        final TableConfig config = tableEnvironment.getConfig();

        // Prevents invalid retractions e.g. through non-deterministic time functions like NOW()
        config.set(
                TABLE_OPTIMIZER_NONDETERMINISTIC_UPDATE_STRATEGY,
                NonDeterministicUpdateStrategy.TRY_RESOLVE);

        // Disable OPTION hints
        config.set(TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, false);

        // The limitation of having just a single rowtime attribute column in the query schema
        // causes confusion. The Kafka sink does not use StreamRecord's timestamps anyway.
        config.set(TABLE_EXEC_SINK_ROWTIME_INSERTER, RowtimeInserter.DISABLED);

        // Metadata virtual columns act as a kind of "system column" in Confluent's SQL dialect.
        // In order to add new system columns at any time, column expansions using `SELECT *` will
        // not select those columns. It avoids downstream schema changes such as
        // `INSERT INTO sink SELECT * FROM source`.
        config.set(
                TABLE_COLUMN_EXPANSION_STRATEGY,
                Arrays.asList(
                        ColumnExpansionStrategy.EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS,
                        ColumnExpansionStrategy.EXCLUDE_ALIASED_VIRTUAL_METADATA_COLUMNS));

        // Job submission service doesn't need to attempt a plan enrichment from catalog.
        // We fully rely on what has been serialized into the compiled plan.
        config.set(PLAN_RESTORE_CATALOG_OBJECTS, CatalogPlanRestore.ALL_ENFORCED);

        // Insert a proxy core module to control which functions get exposed and which ones are
        // forbidden.
        tableEnvironment.unloadModule("core");
        tableEnvironment.loadModule("core", CoreProxyModule.INSTANCE);

        if (service == Service.JOB_SUBMISSION_SERVICE
                || privateConfig.get(ServiceTasksOptions.CONFLUENT_AI_FUNCTIONS_ENABLED)) {
            tableEnvironment.loadModule("openai", AIFunctionsModule.INSTANCE);
        }

        if (service == Service.JOB_SUBMISSION_SERVICE
                || privateConfig.get(ServiceTasksOptions.CONFLUENT_OTLP_FUNCTIONS_ENABLED)) {
            tableEnvironment.loadModule("otlp", OtlpFunctionsModule.INSTANCE);
        }

        if (service == Service.JOB_SUBMISSION_SERVICE
                || privateConfig.get(ServiceTasksOptions.CONFLUENT_REMOTE_UDF_ENABLED)
                // TODO: remove this demo hack...
                || privateConfig.get(ServiceTasksOptions.CONFLUENT_AI_FUNCTIONS_ENABLED)) {
            // Forward the target address of the remote gateway (or proxy) to the udf.
            Map<String, String> remoteUdfConfig = new HashMap<>();
            remoteUdfConfig.put(
                    ServiceTasksOptions.CONFLUENT_REMOTE_UDF_TARGET.key(),
                    privateConfig.getString(ServiceTasksOptions.CONFLUENT_REMOTE_UDF_TARGET));
            tableEnvironment.loadModule("remote_udf", new RemoteUdfModule(remoteUdfConfig));
        }
    }

    private static void validateConfiguration(Configuration providedOptions) {
        // FactoryUtil is actually intended for factories but has very convenient
        // validation capabilities. We can replace this call with something custom
        // if necessary.

        FactoryUtil.validateFactoryOptions(
                Collections.emptySet(), ServiceTasksOptions.ALL_PUBLIC_OPTIONS, providedOptions);

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
        validateUnconsumedKeys(ServiceTasksOptions.ALL_PUBLIC_OPTIONS, providedOptions);
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

        final Set<String> earlyAccessOptionKeys =
                ServiceTasksOptions.EARLY_ACCESS_PUBLIC_OPTIONS.stream()
                        .flatMap(
                                option ->
                                        allKeysExpanded(option, providedOptions.keySet()).stream())
                        .collect(Collectors.toSet());

        final Set<String> remainingOptionKeys = new HashSet<>(providedOptions.keySet());

        // Remove consumed keys
        remainingOptionKeys.removeAll(consumedOptionKeys);

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
                                    // Early access options should not be shown in error messages.
                                    .filter(
                                            k ->
                                                    !deprecatedOptionKeys.contains(k)
                                                            && !earlyAccessOptionKeys.contains(k))
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
                compile(
                        true,
                        tableEnvironment,
                        Collections.singletonList(modifyOperation),
                        connectorOptions);

        if (compilationResult.isLocal()) {
            return new ForegroundLocalResultPlan(
                    compilationResult.querySummary, compilationResult.data);
        }

        final String operatorId =
                extractOperatorId(tableEnvironment.getConfig(), compilationResult.execNodeGraph);

        return new ForegroundJobResultPlan(
                compilationResult.querySummary, compilationResult.compiledPlan, operatorId);
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
    public BackgroundJobResultPlan compileBackgroundQueries(
            TableEnvironment tableEnvironment,
            List<ModifyOperation> modifyOperations,
            ConnectorOptionsProvider connectorOptions) {
        final CompilationResult compilationResult =
                compile(false, tableEnvironment, modifyOperations, connectorOptions);

        if (compilationResult.isLocal()) {
            throw new UnsupportedOperationException(
                    "Local execution is currently not supported for background queries.");
        }

        return new BackgroundJobResultPlan(
                compilationResult.querySummary, compilationResult.compiledPlan);
    }

    // --------------------------------------------------------------------------------------------
    // Common methods
    // --------------------------------------------------------------------------------------------

    private static class CompilationResult {
        final QuerySummary querySummary;
        final Stream<RowData> data;
        final ExecNodeGraph execNodeGraph;
        final String compiledPlan;

        private CompilationResult(
                QuerySummary querySummary,
                ExecNodeGraph execNodeGraph,
                String compiledPlan,
                Stream<RowData> data) {
            this.querySummary = querySummary;
            this.execNodeGraph = execNodeGraph;
            this.compiledPlan = compiledPlan;
            this.data = data;
        }

        static CompilationResult job(
                QuerySummary querySummary, ExecNodeGraph execNodeGraph, String compiledPlan) {
            return new CompilationResult(querySummary, execNodeGraph, compiledPlan, null);
        }

        static CompilationResult local(QuerySummary querySummary, Stream<RowData> data) {
            return new CompilationResult(querySummary, null, null, data);
        }

        boolean isLocal() {
            return data != null;
        }
    }

    private static CompilationResult compile(
            boolean isForeground,
            TableEnvironment tableEnvironment,
            List<ModifyOperation> modifyOperations,
            ConnectorOptionsProvider connectorOptions) {
        final TableEnvironmentImpl tableEnv = (TableEnvironmentImpl) tableEnvironment;

        final StreamPlanner planner = (StreamPlanner) tableEnv.getPlanner();

        final List<FlinkPhysicalRel> physicalGraph = optimize(planner, modifyOperations);

        final QuerySummary querySummary = QuerySummary.summarize(isForeground, physicalGraph);

        final SerdeContext serdeContext = planner.createSerdeContext();

        final Optional<Stream<RowData>> localResults =
                LocalExecution.RULES.stream()
                        .filter(r -> r.matches(querySummary))
                        .findFirst()
                        .flatMap(e -> e.execute(serdeContext, physicalGraph));

        if (localResults.isPresent()) {
            return CompilationResult.local(querySummary, localResults.get());
        }

        final ExecNodeGraph graph = translate(planner, physicalGraph);

        graph.getRootNodes().forEach(node -> exposePrivateConnectorOptions(node, connectorOptions));

        graph.getRootNodes().forEach(DefaultServiceTasks::checkForUnsupportedExecNodes);

        final String compiledPlan;
        try {
            compiledPlan =
                    JsonSerdeUtil.createObjectWriter(serdeContext)
                            .withDefaultPrettyPrinter()
                            .writeValueAsString(graph);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize given ExecNodeGraph", e);
        }

        return CompilationResult.job(querySummary, graph, compiledPlan);
    }

    /** Runs the optimizer and returns a list of {@link FlinkPhysicalRel}. */
    @SuppressWarnings("unchecked")
    private static List<FlinkPhysicalRel> optimize(
            StreamPlanner planner, List<ModifyOperation> operations) {
        final List<RelNode> logicalNodes =
                operations.stream().map(planner::translateToRel).collect(Collectors.toList());
        final List<RelNode> optimizedNodes = toJava(planner.optimize(toScala(logicalNodes)));
        return (List<FlinkPhysicalRel>) (List<?>) optimizedNodes;
    }

    /** Runs the translation to an {@link ExecNodeGraph}. */
    @SuppressWarnings("unchecked")
    private static ExecNodeGraph translate(
            StreamPlanner planner, List<FlinkPhysicalRel> physicalGraph) {
        final List<RelNode> optimizedNodes = (List<RelNode>) (List<?>) physicalGraph;
        return planner.translateToExecNodeGraph(toScala(optimizedNodes), true);
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
