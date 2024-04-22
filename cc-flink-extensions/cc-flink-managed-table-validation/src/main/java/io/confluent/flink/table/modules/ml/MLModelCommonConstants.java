/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

/** Collention of some normal model properties. */
public class MLModelCommonConstants {
    // Common Provider level config constants
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
    public static final String INPUT_FORMAT = "INPUT_FORMAT";
    public static final String OUTPUT_FORMAT = "OUTPUT_FORMAT";
    public static final String INPUT_CONTENT_TYPE = "INPUT_CONTENT_TYPE";
    public static final String OUTPUT_CONTENT_TYPE = "OUTPUT_CONTENT_TYPE";

    // AWS Sagamaker specific constants
    public static final String CUSTOM_ATTRIBUTES = "CUSTOM_ATTRIBUTES";
    public static final String INFERENCE_ID = "INFERENCE_ID";
    public static final String TARGET_VARIANT = "TARGET_VARIANT";
    public static final String TARGET_MODEL = "TARGET_MODEL";
    public static final String TARGET_CONTAINER_HOST_NAME = "TARGET_CONTAINER_HOST_NAME";
    public static final String INFERENCE_COMPONENT_NAME = "INFERENCE_COMPONENT_NAME";
    public static final String ENABLE_EXPLANATIONS = "ENABLE_EXPLANATIONS";

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
