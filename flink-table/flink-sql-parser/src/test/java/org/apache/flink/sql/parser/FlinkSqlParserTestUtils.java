/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.sql.parser;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlNode;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeDiagnosingMatcher;

/** Utilities used in {@link FlinkSqlParserImplTest}. */
@Confluent
public class FlinkSqlParserTestUtils {

    static BaseMatcher<SqlNode> validated(String validatedSql) {
        return new TypeSafeDiagnosingMatcher<SqlNode>() {
            @Override
            protected boolean matchesSafely(SqlNode item, Description mismatchDescription) {
                if (item instanceof ExtendedSqlNode) {
                    try {
                        ((ExtendedSqlNode) item).validate();
                    } catch (SqlValidateException e) {
                        mismatchDescription.appendText(
                                "Could not validate the node. Exception: \n");
                        mismatchDescription.appendValue(e);
                    }

                    String actual = item.toSqlString(null, true).getSql();
                    return actual.equals(validatedSql);
                }
                mismatchDescription.appendText(
                        "This matcher can be applied only to ExtendedSqlNode.");
                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText(
                        "The validated node string representation should be equal to: \n");
                description.appendText(validatedSql);
            }
        };
    }
}
