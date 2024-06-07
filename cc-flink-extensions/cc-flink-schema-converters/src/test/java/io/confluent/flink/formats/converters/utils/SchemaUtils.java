/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.utils;

import io.confluent.flink.formats.converters.protobuf.CommonMappings;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility methods for dealing with schemas. */
public class SchemaUtils {

    /** Reads a schema string from the given resource. */
    public static String readSchemaFromResource(String resourcePath) {
        try {
            return IOUtils.toString(
                    checkNotNull(
                            CommonMappings.class
                                    .getClassLoader()
                                    .getResourceAsStream(resourcePath)),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SchemaUtils() {}
}
