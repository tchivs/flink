/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Objects;

/** The request description for {@link org.apache.flink.runtime.rest.handler.job.JobFailHandler}. */
@Confluent
public class JobFailRequestBody implements RequestBody {

    private static final String FIELD_FAIL_REASON = "failReason";

    @JsonProperty(FIELD_FAIL_REASON)
    @Nullable
    private final String failReason;

    public JobFailRequestBody(@Nullable @JsonProperty(FIELD_FAIL_REASON) String failReason) {
        this.failReason = failReason;
    }

    @Nullable
    public String getFailReason() {
        return failReason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobFailRequestBody that = (JobFailRequestBody) o;
        return Objects.equals(failReason, that.failReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failReason);
    }

    @Override
    public String toString() {
        return "JobFailRequestBody{" + "failReason='" + failReason + '\'' + '}';
    }
}
