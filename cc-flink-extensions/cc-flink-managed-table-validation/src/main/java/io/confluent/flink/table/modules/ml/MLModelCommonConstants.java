/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

/** Collention of some normal model properties. */
public class MLModelCommonConstants {
    public static final String API_KEY = "API_KEY";
    public static final String PROVIDER = "PROVIDER";
    public static final String ENDPOINT = "ENDPOINT";
    public static final String SERVICE_KEY = "SERVICE_KEY";
    public static final String PARAMS_PREFIX = "PARAMS.";
    public static final String DEFAULT_VERSION = "DEFAULT_VERSION";
    public static final String TASK = "TASK";
    public static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    public static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";
    public static final String AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN";
    public static final String SYSTEM_PROMPT = "SYSTEM_PROMPT";

    // Below are config names used internally. Shouldn't be set by user
    public static final String CONFLUENT_PRIVATE_PREFIX = "CONFLUENT";

    public static final String ENCRYPT_STRATEGY = "CONFLUENT.MODEL.SECRET.ENCRYPT_STRATEGY";
    public static final String ORG_ID = "CONFLUENT.MODEL.ORG.ID";
    public static final String ENV_ID = "CONFLUENT.MODEL.ENV.ID";
    public static final String DATABASE_ID = "CONFLUENT.MODEL.DATABASE.ID";
    public static final String COMPUTE_POOL_ID = "CONFLUENT.MODEL.COMPUTE_POOL.ID";
    public static final String MODEL_NAME = "CONFLUENT.MODEL.NAME";
    public static final String MODEL_VERSION = "CONFLUENT.MODEL.VERSION";
    public static final String MODEL_KMS_KEY_VERSION_PREFIX = "CONFLUENT.MODEL.SECRET.KEY_VERSION";
}
