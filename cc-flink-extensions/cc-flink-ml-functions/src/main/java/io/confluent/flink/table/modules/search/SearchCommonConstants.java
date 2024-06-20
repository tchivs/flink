/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search;

/** Collection of some normal searchable external table properties. */
public class SearchCommonConstants {
    // Common Provider level config constants
    public static final String API_KEY = "API_KEY";
    public static final String PROVIDER = "PROVIDER";
    public static final String ENDPOINT = "ENDPOINT";
    public static final String SERVICE_KEY = "SERVICE_KEY";
    public static final String PARAMS_PREFIX = "PARAMS.";
    public static final String DEFAULT_VERSION = "DEFAULT_VERSION";

    // Below are config names used internally. Shouldn't be set by user
    public static final String CONFLUENT_PRIVATE_PREFIX = "CONFLUENT";
    public static final String ENCRYPT_STRATEGY =
            "CONFLUENT.EXTERNAL_TABLE.SECRET.ENCRYPT_STRATEGY";
    public static final String ORG_ID = "CONFLUENT.EXTERNAL_TABLE.ORG.ID";
    public static final String ENV_ID = "CONFLUENT.EXTERNAL_TABLE.ENV.ID";
    public static final String DATABASE_ID = "CONFLUENT.EXTERNAL_TABLE.DATABASE.ID";
    public static final String COMPUTE_POOL_ID = "CONFLUENT.EXTERNAL_TABLE.COMPUTE_POOL.ID";
    public static final String COMPUTE_POOL_ENV_ID = "CONFLUENT.EXTERNAL_TABLE.COMPUTE_POOL.ENV.ID";
    public static final String EXTERNAL_TABLE_NAME = "CONFLUENT.EXTERNAL_TABLE.NAME";
    public static final String CREDENTIAL_SERVICE_HOST = "CONFLUENT.CREDENTIAL.SERVICE.HOST";
    public static final String CREDENTIAL_SERVICE_PORT = "CONFLUENT.CREDENTIAL.SERVICE.PORT";
    public static final String KMS_SECRET_RETRY_COUNT =
            "CONFLUENT.CREDENTIAL.KMS.SECRET.RETRY_COUNT";
    public static final String KMS_SECRET_RETRY_DELAY_MS =
            "CONFLUENT.CREDENTIAL.KMS.SECRET.DELAY_MS";
}
