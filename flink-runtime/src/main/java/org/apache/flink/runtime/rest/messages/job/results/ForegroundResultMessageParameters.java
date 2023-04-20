/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.messages.job.results;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.OperatorIDPathParameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/** {@link MessageParameters} to retrieve foreground query results. */
@Confluent
public class ForegroundResultMessageParameters extends MessageParameters {

    private final JobIDPathParameter jobPathParameter = new JobIDPathParameter();
    private final OperatorIDPathParameter operatorPathParameter = new OperatorIDPathParameter();

    @Override
    public Collection<MessagePathParameter<?>> getPathParameters() {
        return Arrays.asList(jobPathParameter, operatorPathParameter);
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.emptyList();
    }
}
