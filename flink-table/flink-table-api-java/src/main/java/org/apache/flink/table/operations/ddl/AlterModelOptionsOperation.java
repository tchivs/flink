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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.ModelChange;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import java.util.stream.Collectors;

/** Operation to describe a ALTER MODEL .. SET .. statement. */
public class AlterModelOptionsOperation implements AlterOperation {
    private final ObjectIdentifier modelIdentifier;
    private final CatalogModel catalogModel;
    private final boolean ignoreIfNotExists;

    public ObjectIdentifier getModelIdentifier() {
        return modelIdentifier;
    }

    public CatalogModel getCatalogModel() {
        return catalogModel;
    }

    public boolean ignoreIfNotExists() {
        return ignoreIfNotExists;
    }

    public AlterModelOptionsOperation(
            ObjectIdentifier modelIdentifier,
            CatalogModel catalogModel,
            boolean ignoreIfNotExists) {
        this.modelIdentifier = modelIdentifier;
        this.catalogModel = catalogModel;
        this.ignoreIfNotExists = ignoreIfNotExists;
    }

    @Override
    public String asSummaryString() {
        String changes =
                catalogModel.getModelChanges().stream()
                        .map(this::toString)
                        .collect(Collectors.joining(",\n"));
        return String.format(
                "ALTER MODEL %s%s\n%s",
                ignoreIfNotExists ? "IF EXISTS " : "", modelIdentifier.asSummaryString(), changes);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ctx.getCatalogManager()
                .alterModel(getCatalogModel(), getModelIdentifier(), ignoreIfNotExists());
        return TableResultImpl.TABLE_RESULT_OK;
    }

    private String toString(ModelChange modelChange) {
        if (modelChange instanceof ModelChange.SetOption) {
            ModelChange.SetOption setOption = (ModelChange.SetOption) modelChange;
            return String.format(
                    "  SET (%s)",
                    OperationUtils.formatParameter(setOption.getKey(), setOption.getValue()));
        } else if (modelChange instanceof ModelChange.ResetOption) {
            ModelChange.ResetOption resetOption = (ModelChange.ResetOption) modelChange;
            return String.format("  RESET (%s)", resetOption.getKey());
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unknown model change: %s", modelChange));
        }
    }
}
