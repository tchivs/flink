/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service.summary;

import org.apache.flink.annotation.Confluent;

/** Global characteristics of a query. */
@Confluent
public enum QueryProperty {
    /** Contains only a single sink (i.e. from INSERT INTO or result serving SELECT) */
    SINGLE_SINK,

    /** Contains multiple sinks (i.e. using EXECUTE STATEMENT SET) */
    MULTI_SINK,

    /** SELECT statement with result serving sink attached. */
    FOREGROUND,

    /** INSERT INTO or EXECUTE STATEMENT SET. */
    BACKGROUND,

    /** All scan sources are bounded, thus, the query itself is bounded. */
    BOUNDED,

    /** There is at least one scan source that is not bounded. */
    UNBOUNDED,

    /**
     * Sinks (regardless of INSERT INTO, result serving SELECT, or EXECUTE STATEMENT SET) will only
     * receive insert only changes. Independent of what the connector is able to digest.
     */
    APPEND_ONLY,

    /**
     * Sinks (regardless of INSERT INTO, result serving SELECT, or EXECUTE STATEMENT SET) will
     * receive updating results. Either retract, upsert, or both (in case of statement set).
     */
    UPDATING
}
