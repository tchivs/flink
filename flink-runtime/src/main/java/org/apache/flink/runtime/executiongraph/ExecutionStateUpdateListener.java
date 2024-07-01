/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.ExecutionState;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A listener that is called when an execution switched to a new state. Should only be called from
 * the {@link org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor
 * ComponentMainThreadExecutor}
 */
@NotThreadSafe
public interface ExecutionStateUpdateListener {
    void onStateUpdate(Execution execution, ExecutionState previousState, ExecutionState newState);

    ExecutionStateUpdateListener NO_OP = (execution, previousState, newState) -> {};

    static ExecutionStateUpdateListener combine(ExecutionStateUpdateListener... lsnrs) {
        return (execution, previousState, newState) -> {
            for (ExecutionStateUpdateListener listener : lsnrs) {
                listener.onStateUpdate(execution, previousState, newState);
            }
        };
    }
}
