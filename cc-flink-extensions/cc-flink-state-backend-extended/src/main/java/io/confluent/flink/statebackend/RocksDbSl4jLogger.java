/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.statebackend;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.MdcUtils;

import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

class RocksDbSl4jLogger extends org.rocksdb.Logger {
    private static final Logger logger = LoggerFactory.getLogger(RocksDbSl4jLogger.class);
    private final Map<String, String> contextData;

    public RocksDbSl4jLogger(DBOptions options, JobID jobID) {
        this(options, MdcUtils.asContextData(jobID));
    }

    public RocksDbSl4jLogger(DBOptions options, Map<String, String> contextData) {
        super(options);
        this.contextData = contextData;
    }

    @Override
    protected void log(InfoLogLevel logLevel, String message) {
        try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(contextData)) {
            switch (logLevel) {
                case FATAL_LEVEL:
                case ERROR_LEVEL:
                    logger.error(message);
                    break;
                case WARN_LEVEL:
                    logger.warn(message);
                    break;
                case INFO_LEVEL:
                    // temporarily decrease logging
                    // see https://confluentinc.atlassian.net/browse/NGN-331
                    // see also https://confluentinc.atlassian.net/browse/FLINKCC-1204
                    //                    logger.info(message);
                    logger.debug(message);
                    break;
                case DEBUG_LEVEL:
                    logger.debug(message);
                    break;
                default:
                    logger.trace(message);
            }
        }
    }
}
