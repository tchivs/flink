/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure.util;

import org.apache.flink.util.StringUtils;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for building user-friendly error messages. Largely adapted from:
 * https://github.com/confluentinc/ksql/blob/master/ksqldb-common/src/main/java/io/confluent/ksql/util/ErrorMessageUtil.java
 */
public final class FailureMessageUtil {

    private static final String PREFIX = "Caused by: ";

    /**
     * Build a failure message containing the message of each throwable in the chain.
     *
     * <p>Throwable messages are separated by new lines.
     *
     * @param throwable the top level error.
     * @return the error message.
     */
    public static String buildMessage(final Throwable throwable) {
        if (throwable == null) {
            return "";
        }

        final List<String> messages = dedup(getMessages(throwable));

        final String msg = messages.remove(0);

        final String causeMsg =
                messages.stream()
                        .filter(s -> !s.isEmpty())
                        .map(
                                cause ->
                                        FailureMessageUtil.wrap(
                                                PREFIX + cause, 80, "\n\t", true, " "))
                        .collect(Collectors.joining(System.lineSeparator()));

        return causeMsg.isEmpty() ? msg : msg + System.lineSeparator() + causeMsg;
    }

    /**
     * Build a list containing the failure message for each throwable in the chain.
     *
     * @param e the top level failure.
     * @return the list of failure messages.
     */
    public static List<String> getMessages(final Throwable e) {
        return getThrowableList(e).stream()
                .map(FailureMessageUtil::getMessage)
                .collect(Collectors.toList());
    }

    private static String getMessage(final Throwable e) {
        if (e instanceof ConnectException) {
            return "Could not connect to the server. "
                    + "Please check the server details are correct and that the server is running.";
        } else {
            final String message = e.getMessage();
            return message == null ? e.toString() : message;
        }
    }

    private static List<Throwable> getThrowableList(final Throwable e) {
        final List<Throwable> list = new ArrayList<>();
        Throwable cause = e;
        while (cause != null && !list.contains(cause)) {
            list.add(cause);
            cause = cause.getCause();
        }
        return list;
    }

    private static List<String> dedup(final List<String> messages) {
        final List<String> dedupedMessages = new ArrayList<>(new LinkedHashSet<>(messages));
        final String message = dedupedMessages.get(0);
        dedupedMessages.subList(1, dedupedMessages.size()).removeIf(message::contains);
        return dedupedMessages;
    }

    /**
     * Wraps a single line of text, identifying words by {@code wrapOn}. Copied over from
     * org.apache.commons.text.WordUtils to avoid the extra dependency see
     * https://confluentinc.atlassian.net/browse/FLINKCC-432
     *
     * <p>Leading spaces on a new line are stripped. Trailing spaces are not stripped.
     *
     * @param str the String to be word wrapped, may be null
     * @param wrapLength the column to wrap the words at, less than 1 is treated as 1
     * @param newLineStr the string to insert for a new line, {@code null} uses the system property
     *     line separator
     * @param wrapLongWords true if long words (such as URLs) should be wrapped
     * @param wrapOn regex expression to be used as a breakable characters, if blank string is
     *     provided a space character will be used
     * @return a line with newlines inserted, {@code null} if null input
     */
    public static String wrap(
            final String str,
            int wrapLength,
            String newLineStr,
            final boolean wrapLongWords,
            String wrapOn) {
        if (str == null) {
            return null;
        }
        if (newLineStr == null) {
            newLineStr = System.lineSeparator();
        }
        if (wrapLength < 1) {
            wrapLength = 1;
        }
        if (StringUtils.isNullOrWhitespaceOnly(wrapOn)) {
            wrapOn = " ";
        }
        final Pattern patternToWrapOn = Pattern.compile(wrapOn);
        final int inputLineLength = str.length();
        int offset = 0;
        final StringBuilder wrappedLine = new StringBuilder(inputLineLength + 32);
        int matcherSize = -1;

        while (offset < inputLineLength) {
            int spaceToWrapAt = -1;
            Matcher matcher =
                    patternToWrapOn.matcher(
                            str.substring(
                                    offset,
                                    Math.min(
                                            (int)
                                                    Math.min(
                                                            Integer.MAX_VALUE,
                                                            offset + wrapLength + 1L),
                                            inputLineLength)));
            if (matcher.find()) {
                if (matcher.start() == 0) {
                    matcherSize = matcher.end();
                    if (matcherSize != 0) {
                        offset += matcher.end();
                        continue;
                    }
                    offset += 1;
                }
                spaceToWrapAt = matcher.start() + offset;
            }

            // only last line without leading spaces is left
            if (inputLineLength - offset <= wrapLength) {
                break;
            }

            while (matcher.find()) {
                spaceToWrapAt = matcher.start() + offset;
            }

            if (spaceToWrapAt >= offset) {
                // normal case
                wrappedLine.append(str, offset, spaceToWrapAt);
                wrappedLine.append(newLineStr);
                offset = spaceToWrapAt + 1;

            } else // really long word or URL
            if (wrapLongWords) {
                if (matcherSize == 0) {
                    offset--;
                }
                // wrap really long word one line at a time
                wrappedLine.append(str, offset, wrapLength + offset);
                wrappedLine.append(newLineStr);
                offset += wrapLength;
                matcherSize = -1;
            } else {
                // do not wrap really long word, just extend beyond limit
                matcher = patternToWrapOn.matcher(str.substring(offset + wrapLength));
                if (matcher.find()) {
                    matcherSize = matcher.end() - matcher.start();
                    spaceToWrapAt = matcher.start() + offset + wrapLength;
                }

                if (spaceToWrapAt >= 0) {
                    if (matcherSize == 0 && offset != 0) {
                        offset--;
                    }
                    wrappedLine.append(str, offset, spaceToWrapAt);
                    wrappedLine.append(newLineStr);
                    offset = spaceToWrapAt + 1;
                } else {
                    if (matcherSize == 0 && offset != 0) {
                        offset--;
                    }
                    wrappedLine.append(str, offset, str.length());
                    offset = inputLineLength;
                    matcherSize = -1;
                }
            }
        }

        if (matcherSize == 0 && offset < inputLineLength) {
            offset--;
        }

        // Whatever is left in line is short enough to just pass through
        wrappedLine.append(str, offset, str.length());

        return wrappedLine.toString();
    }
}
