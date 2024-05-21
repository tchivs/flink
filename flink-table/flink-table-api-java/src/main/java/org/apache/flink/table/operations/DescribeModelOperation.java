/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.operations;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.types.DataType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.table.api.internal.TableResultUtils.buildTableResult;

/**
 * Operation to describe a DESCRIBE [EXTENDED] [[catalogName.] dataBasesName].sqlIdentifier
 * statement.
 */
public class DescribeModelOperation implements Operation, ExecutableOperation {

    private final ObjectIdentifier sqlIdentifier;
    private final boolean isExtended;

    public DescribeModelOperation(ObjectIdentifier sqlIdentifier, boolean isExtended) {
        this.sqlIdentifier = sqlIdentifier;
        this.isExtended = isExtended;
    }

    public ObjectIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }

    public boolean isExtended() {
        return isExtended;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", sqlIdentifier);
        params.put("isExtended", isExtended);
        return OperationUtils.formatWithChildren(
                "DESCRIBE MODEL", params, Collections.emptyList(), Operation::asSummaryString);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        // DESCRIBE MODEL <model> shows input/output schema if any.
        ResolvedCatalogModel model = ctx.getCatalogManager().getModelOrError(sqlIdentifier);
        final String[] headers = {"inputs", "outputs"};
        final DataType[] types = {DataTypes.STRING(), DataTypes.STRING()};
        // We only ever return a single row.
        Object[][] rows = {
            {model.getResolvedInputSchema().toString(), model.getResolvedOutputSchema().toString()}
        };
        return buildTableResult(headers, types, rows);
    }
}
