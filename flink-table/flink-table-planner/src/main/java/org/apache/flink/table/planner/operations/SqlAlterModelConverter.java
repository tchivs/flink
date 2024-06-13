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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlAlterModel;
import org.apache.flink.sql.parser.ddl.SqlAlterModelRename;
import org.apache.flink.sql.parser.ddl.SqlAlterModelSet;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterModelOptionsOperation;
import org.apache.flink.table.operations.ddl.AlterModelRenameOperation;

import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Helper class for converting {@link SqlAlterModel} to {@link AlterModelOptionsOperation}. */
public class SqlAlterModelConverter {
    private final CatalogManager catalogManager;

    SqlAlterModelConverter(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public Operation convertAlterModel(SqlAlterModel sqlAlterModel) {
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterModel.fullModelName());
        ObjectIdentifier modelIdentifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        if (sqlAlterModel instanceof SqlAlterModelRename) {
            SqlAlterModelRename sqlAlterModelRename = (SqlAlterModelRename) sqlAlterModel;
            // Rename model
            UnresolvedIdentifier newUnresolvedIdentifier =
                    UnresolvedIdentifier.of(sqlAlterModelRename.fullNewModelName());
            ObjectIdentifier newModelIdentifier =
                    catalogManager.qualifyIdentifier(newUnresolvedIdentifier);
            return new AlterModelRenameOperation(
                    modelIdentifier, newModelIdentifier, sqlAlterModel.ifModelExists());
        } else if (sqlAlterModel instanceof SqlAlterModelSet) {
            SqlAlterModelSet sqlAlterModelSet = (SqlAlterModelSet) sqlAlterModel;
            Map<String, String> changeModelOptions = getModelOptions(sqlAlterModelSet);
            return new AlterModelOptionsOperation(
                    modelIdentifier,
                    CatalogModel.of(
                            Schema.newBuilder().build(),
                            Schema.newBuilder().build(),
                            changeModelOptions,
                            null),
                    sqlAlterModel.ifModelExists());
        } else {
            throw new ValidationException(
                    String.format(
                            "[%s] needs to implement",
                            sqlAlterModel.toSqlString(CalciteSqlDialect.DEFAULT)));
        }
    }

    private Map<String, String> getModelOptions(SqlAlterModelSet sqlAlterModelSet) {
        Map<String, String> options = new HashMap<>();
        sqlAlterModelSet
                .getModelOptionList()
                .getList()
                .forEach(
                        p ->
                                options.put(
                                        ((SqlTableOption) Objects.requireNonNull(p))
                                                .getKeyString()
                                                .toUpperCase(),
                                        ((SqlTableOption) p).getValueString()));
        return options;
    }
}
