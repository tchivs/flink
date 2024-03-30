/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.credentials;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Collections;
import java.util.Map;

/** The job options that are submitted along with a job to fetch credentials. */
@Confluent
public class JobOptions {

    public static final ConfigOption<String> IDENTITY_POOL_ID =
            ConfigOptions.key("job.identity.pool.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The identity pool of the job");

    public static final ConfigOption<String> COMPUTE_POOL_ID =
            ConfigOptions.key("job.compute.pool.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The compute pool of the job");

    public static final ConfigOption<String> STATEMENT_ID_CRN =
            ConfigOptions.key("job.statement.id.crn")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The statement id of the job");

    public static final ConfigOption<String> COMMA_SEPARATED_PRINCIPALS =
            ConfigOptions.key("job.principal.ids")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The comma separated principals of the job");

    public static final ConfigOption<Map<String, String>> CONFLUENT_UDF_PREFIX =
            ConfigOptions.key("confluent.functions")
                    .mapType()
                    .defaultValue(Collections.emptyMap())
                    .withDescription("The udf prefix for metadata submitted with the job");
}
