/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.util.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Confluent
class ResourceUtils {

    public static String loadResource(String name) throws IOException {
        InputStream in = ResourceUtils.class.getResourceAsStream(name);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copyBytes(in, out);
        return new String(out.toByteArray(), StandardCharsets.UTF_8);
    }
}
