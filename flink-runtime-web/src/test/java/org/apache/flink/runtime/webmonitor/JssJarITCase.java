/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.test.junit5.InjectClusterRESTAddress;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.fail;

/** Test that the jss integration jar is served properly. */
@ExtendWith(MiniClusterExtension.class)
class JssJarITCase {

    private static final Logger LOG = LoggerFactory.getLogger(JssJarITCase.class);

    @Test
    void getJssJar(@InjectClusterRESTAddress URI restAddress) throws Exception {
        assertEventuallyAvailable(
                restAddress.toASCIIString() + "/assets/cc/flink-integration-impl.jar");
    }

    private static void assertEventuallyAvailable(String url) throws Exception {
        final URL u = new URL(url);
        LOG.info("Accessing URL " + url + " as URL: " + u);

        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10L));

        while (deadline.hasTimeLeft()) {
            HttpURLConnection connection = (HttpURLConnection) u.openConnection();
            connection.setConnectTimeout(100000);
            connection.connect();

            int responseCode = connection.getResponseCode();

            if (Objects.equals(
                    HttpResponseStatus.SERVICE_UNAVAILABLE,
                    HttpResponseStatus.valueOf(responseCode))) {
                // service not available --> Sleep and retry
                LOG.debug("Web service currently not available. Retrying the request in a bit.");
                Thread.sleep(100L);
            } else {
                if (responseCode == HttpResponseStatus.OK.code()) {
                    return;
                } else {
                    fail(
                            String.format(
                                    "Failed to fetch integration jar. Response code: %s",
                                    responseCode));
                }
            }
        }

        throw new TimeoutException(
                "Could not get HTTP response in time since the service is still unavailable.");
    }
}
