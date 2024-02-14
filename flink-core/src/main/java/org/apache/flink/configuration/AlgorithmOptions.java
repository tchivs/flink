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

package org.apache.flink.configuration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.TaskManagerOptions.MEMORY_SEGMENT_SIZE;

/** Configuration parameters for join/sort algorithms. */
public class AlgorithmOptions {

    public static final ConfigOption<Boolean> HASH_JOIN_BLOOM_FILTERS =
            key("taskmanager.runtime.hashjoin-bloom-filters")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Flag to activate/deactivate bloom filters in the hybrid hash join implementation."
                                    + " In cases where the hash join needs to spill to disk (datasets larger than the reserved fraction of"
                                    + " memory), these bloom filters can greatly reduce the number of spilled records, at the cost some"
                                    + " CPU cycles.");

    public static final ConfigOption<Integer> SPILLING_MAX_FAN =
            key("taskmanager.runtime.max-fan")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "The maximal fan-in for external merge joins and fan-out for spilling hash tables. Limits"
                                    + " the number of file handles per operator, but may cause intermediate merging/partitioning, if set too"
                                    + " small.");

    public static final ConfigOption<Float> SORT_SPILLING_THRESHOLD =
            key("taskmanager.runtime.sort-spilling-threshold")
                    .floatType()
                    .defaultValue(0.8f)
                    .withDescription(
                            "A sort operation starts spilling when this fraction of its memory budget is full.");

    public static final ConfigOption<Boolean> USE_LARGE_RECORDS_HANDLER =
            key("taskmanager.runtime.large-record-handler")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to use the LargeRecordHandler when spilling. If a record will not fit into the sorting"
                                    + " buffer. The record will be spilled on disk and the sorting will continue with only the key."
                                    + " The record itself will be read afterwards when merging.");

    public static final ConfigOption<MemorySize> GLOBAL_AGG_BUFFER_SIZE =
            ConfigOptions.key("taskmanager.runtime.aggregation.global.buffer-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum size of the buffer used by Global Aggregation."
                                    + "The buffer holds records for aggregation in memory and is flushed to state on: "
                                    + "1) checkpoints, 2) watermarks, or 3) when it is full."
                                    + "If it's too big, and watermarks are not progressing and/or the window "
                                    + "is big enough, there will be too much work to be done during the checkpoint sync phase."
                                    + "That might lead to checkpoint timeouts. "
                                    + "If it's too small, aggregation efficiency will be lower.");

    public static final ConfigOption<Integer> GLOBAL_AGG_MAX_BUFFERED_RECORDS =
            ConfigOptions.key("taskmanager.runtime.aggregation.global.max-buffered-records")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number of records to buffer in Global Aggregation."
                                    + "see "
                                    + GLOBAL_AGG_BUFFER_SIZE.key()
                                    + " for more details.");

    public static final ConfigOption<MemorySize> LOCAL_AGG_BUFFER_SIZE =
            ConfigOptions.key("taskmanager.runtime.aggregation.local.buffer-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum size of the buffer used by Local Aggregation."
                                    + "Minimum is 128Kb; must be a multiple of "
                                    + MEMORY_SEGMENT_SIZE.key()
                                    + "The buffer holds records for aggregation in memory and is flushed downstream on: "
                                    + "1) checkpoints, 2) watermarks, or 3) when it is full."
                                    + "If it's too big (even if watermarks are progressing), "
                                    + "there might be much more data to flush than the memory available (classic flatMap problem)."
                                    + "That might lead to back-pressure and hard-blocking the task thread."
                                    + "To overcome this, the buffer AND the headers must fit into a network buffer "
                                    + "(see "
                                    + MEMORY_SEGMENT_SIZE.key()
                                    + ")"
                                    + "However, this buffer can't be as small because keys, values, and offsets use separate memory pages.");

    public static final ConfigOption<Integer> LOCAL_AGG_MAX_BUFFERED_RECORDS =
            ConfigOptions.key("taskmanager.runtime.aggregation.local.max-buffered-records")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number of records to buffer in Local Aggregation."
                                    + "see "
                                    + LOCAL_AGG_BUFFER_SIZE.key()
                                    + " for more details.");
}
