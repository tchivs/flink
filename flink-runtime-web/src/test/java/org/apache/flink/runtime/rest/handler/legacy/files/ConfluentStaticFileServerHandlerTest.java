/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.rest.handler.legacy.files;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerConfluentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.webmonitor.testutils.HttpUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the serving of integration jars. */
@Confluent
class ConfluentStaticFileServerHandlerTest {
    @Test
    void testForcedWebUiDirectory(@TempDir Path tmp) throws Exception {
        final Path finalWebUIDir = tmp.resolve("flink-web-ui");
        final Path fileToServe = Paths.get("foo", "bar", "file");
        final Path diskFilePath = finalWebUIDir.resolve(fileToServe);
        final String fileContent = "hello world";

        Files.createDirectories(finalWebUIDir.resolve(diskFilePath).getParent());
        Files.write(diskFilePath, fileContent.getBytes(StandardCharsets.UTF_8));

        final Configuration configuration =
                new Configuration()
                        .set(RestOptions.PORT, 0)
                        .set(
                                JobManagerConfluentOptions.FORCED_WEB_TMP_UI_DIR,
                                tmp.toAbsolutePath().toString());

        try (final StandaloneSessionClusterEntrypoint clusterEntrypoint =
                new StandaloneSessionClusterEntrypoint(configuration)) {
            clusterEntrypoint.startCluster();

            int restPort = clusterEntrypoint.getRestPort();

            int lastResponseCode;
            do {
                lastResponseCode = HttpUtils.getFromHTTP("http://localhost:" + restPort).f0;
                Thread.sleep(5L);
            } while (lastResponseCode != 200);

            final Tuple2<Integer, String> response =
                    HttpUtils.getFromHTTP("http://localhost:" + restPort + "/" + fileToServe);

            assertThat(response.f0).isEqualTo(200);
            assertThat(response.f1).isEqualTo(fileContent);
        }
    }
}
