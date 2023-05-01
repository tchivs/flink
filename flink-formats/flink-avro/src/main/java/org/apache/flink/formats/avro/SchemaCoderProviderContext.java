/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.common.CommonConfluentContext;

/** Context used for getting a {@link SchemaCoder} which contains some runtime information. */
@Confluent
public interface SchemaCoderProviderContext extends CommonConfluentContext {}
