/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.connectors;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import io.confluent.flink.table.infoschema.InfoSchemaTables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** {@link DynamicTableSource} for powering {@link InfoSchemaTables}. */
@Confluent
public class InfoSchemaTableSource implements ScanTableSource, SupportsFilterPushDown {

    private final String tableName;
    private final Set<String> filterColumns;

    private final Map<String, String> pushedFilters = new HashMap<>();

    InfoSchemaTableSource(String tableName) {
        this.tableName = tableName;
        this.filterColumns = InfoSchemaTables.getFilterColumns(tableName);
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getPushedFilters() {
        return pushedFilters;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        final List<ResolvedExpression> accepted = new ArrayList<>();
        final List<ResolvedExpression> remaining = new ArrayList<>();

        filters.forEach(
                filter -> {
                    if (extractFilterColumn(filter)) {
                        accepted.add(filter);
                    } else {
                        remaining.add(filter);
                    }
                });

        return Result.of(accepted, remaining);
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        // Currently, there is no runtime provided.
        // Once we support complex INFORMATION_SCHEMA queries that require a Flink job,
        // we can add an implementation here.
        return () -> true;
    }

    @Override
    public DynamicTableSource copy() {
        final InfoSchemaTableSource copy = new InfoSchemaTableSource(tableName);
        copy.pushedFilters.putAll(pushedFilters);
        return copy;
    }

    @Override
    public String asSummaryString() {
        return InfoSchemaTableFactory.class.getSimpleName();
    }

    // --------------------------------------------------------------------------------------------
    // Predicate extraction
    // --------------------------------------------------------------------------------------------

    private boolean extractFilterColumn(ResolvedExpression expression) {
        // Check for a EQUALS call
        if (!(expression instanceof CallExpression)) {
            return false;
        }
        final CallExpression call = (CallExpression) expression;
        if (call.getFunctionDefinition() != BuiltInFunctionDefinitions.EQUALS) {
            return false;
        }

        // Extract ID columns if possible (column can be on the left or right side)
        final List<ResolvedExpression> args = call.getResolvedChildren();
        if (extractFilterColumnFromArgs(args.get(0), args.get(1))) {
            return true;
        }
        return extractFilterColumnFromArgs(args.get(1), args.get(0));
    }

    private boolean extractFilterColumnFromArgs(
            ResolvedExpression column, ResolvedExpression value) {
        // Check arguments
        if (!(column instanceof FieldReferenceExpression)) {
            return false;
        }
        final FieldReferenceExpression field = (FieldReferenceExpression) column;
        if (!(value instanceof ValueLiteralExpression)) {
            return false;
        }
        final ValueLiteralExpression literal = (ValueLiteralExpression) value;

        // Check whether the column is a filter column
        final String columnName = field.getName();
        if (!filterColumns.contains(columnName)) {
            return false;
        }

        // Extract the value
        final Optional<String> optionalValue = literal.getValueAs(String.class);
        if (!optionalValue.isPresent()) {
            return false;
        }

        pushedFilters.put(field.getName(), optionalValue.get());
        return true;
    }
}
