/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure.util;

import org.junit.Test;

import java.net.ConnectException;
import java.sql.SQLDataException;
import java.util.Arrays;

import static io.confluent.flink.runtime.failure.util.FailureMessageUtil.buildMessage;
import static io.confluent.flink.runtime.failure.util.FailureMessageUtil.getMessages;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link FailureMessageUtil}. Largely adapted from:
 * https://github.com/confluentinc/ksql/blob/master/ksqldb-common/src/test/java/io/confluent/ksql/util/ErrorMessageUtilTest.java
 */
public class FailureMessageUtilTest {

    @Test
    public void shouldBuildSimpleErrorMessage() {
        assertThat(
                buildMessage(new SQLDataException("some message", "some state", 422)),
                is("some message"));
    }

    @Test
    public void shouldUseCustomMsgForConnectException() {
        assertThat(
                buildMessage(new ConnectException("asdf")),
                is(
                        "Could not connect to the server. Please check the server details are correct and that the server is running."));
    }

    @Test
    public void shouldBuildErrorMessageFromExceptionWithNoMessage() {
        assertThat(buildMessage(new NullPointerException()), is("java.lang.NullPointerException"));
    }

    @Test
    public void shouldBuildErrorMessageFromExceptionChain() {
        final Throwable cause = new TestException("Something went wrong");
        final Throwable subLevel2 = new TestException("Intermediate message 2", cause);
        final Throwable subLevel1 = new TestException("Intermediate message 1", subLevel2);
        final Throwable e = new TestException("Top level", subLevel1);

        assertThat(
                buildMessage(e),
                is(
                        "Top level"
                                + System.lineSeparator()
                                + "Caused by: Intermediate message 1"
                                + System.lineSeparator()
                                + "Caused by: Intermediate message 2"
                                + System.lineSeparator()
                                + "Caused by: Something went wrong"));
    }

    @Test
    public void shouldDeduplicateMessage() {
        final Throwable cause = new TestException("Something went wrong");
        final Throwable subLevel3 = new TestException("Something went wrong", cause);
        final Throwable subLevel2 = new TestException("Msg that matches", subLevel3);
        final Throwable subLevel1 = new TestException("Msg that matches", subLevel2);
        final Throwable e = new TestException("Msg that matches", subLevel1);

        assertThat(
                buildMessage(e),
                is(
                        "Msg that matches"
                                + System.lineSeparator()
                                + "Caused by: Something went wrong"));
    }

    @Test
    public void shouldNotDeduplicateMessageIfNextMessageIsLonger() {
        final Throwable cause = new TestException("Something went wrong");
        final Throwable subLevel1 = new TestException("Some Message with more detail", cause);
        final Throwable e = new TestException("Some Message", subLevel1);

        assertThat(
                buildMessage(e),
                is(
                        "Some Message"
                                + System.lineSeparator()
                                + "Caused by: Some Message with more detail"
                                + System.lineSeparator()
                                + "Caused by: Something went wrong"));
    }

    @Test
    public void shouldRemoveSubMessages() {
        final Throwable cause = new TestException("Sub-message2");
        final Throwable subLevel1 = new TestException("This is Sub-message1", cause);
        final Throwable e =
                new TestException(
                        "The Main Message that Contains Sub-message1 and Sub-message2", subLevel1);

        assertThat(
                buildMessage(e),
                is(
                        "The Main Message that Contains Sub-message1 and Sub-message2"
                                + System.lineSeparator()
                                + "Caused by: This is Sub-message1"));
    }

    @Test
    public void shouldHandleRecursiveExceptions() {
        assertThat(buildMessage(new RecursiveException("It went boom")), is("It went boom"));
    }

    @Test
    public void shouldHandleRecursiveExceptionChain() {
        final Exception cause = new TestException("Something went wrong");
        final Throwable e = new TestException("Top level", cause);
        cause.initCause(e);

        assertThat(
                buildMessage(e),
                is("Top level" + System.lineSeparator() + "Caused by: Something went wrong"));
    }

    @Test
    public void shouldBuildErrorMessageChain() {
        // Given:
        final Throwable e = new Exception("root", new Exception("cause"));

        // Then:
        assertThat(getMessages(e), equalTo(Arrays.asList("root", "cause")));
    }

    private static class TestException extends Exception {

        private TestException(final String msg) {
            super(msg);
        }

        private TestException(final String msg, final Throwable cause) {
            super(msg, cause);
        }
    }

    private static class RecursiveException extends Exception {

        private RecursiveException(final String message) {
            super(message);
        }

        public synchronized Throwable getCause() {
            return this;
        }
    }
}
