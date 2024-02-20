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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/** Utility for deciding whether than expression can be reduced or not. */
public class ReducerUtil {

    /**
     * Checks whether it contains any function calls which don't want to be reduced.
     *
     * @param node the RexNode to check
     * @return true if it contains any non-reducible function calls in the specified node.
     */
    public static boolean canReduceExpression(RexNode node) {
        return node.accept(new CanReduceExpressionVisitor());
    }

    private static class CanReduceExpressionVisitor extends RexDefaultVisitor<Boolean> {

        @Override
        public Boolean visitNode(RexNode rexNode) {
            return true;
        }

        private boolean canReduceExpression(RexCall call) {
            FunctionDefinition definition = ShortcutUtils.unwrapFunctionDefinition(call);
            return definition == null || definition.canReduceExpression();
        }

        @Override
        public Boolean visitCall(RexCall call) {
            boolean canReduceExpression = canReduceExpression(call);
            return canReduceExpression
                    && (call.getOperands().stream().allMatch(node -> node.accept(this)));
        }
    }
}
