/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/** Classified exception with a message that can be exposed to the user. */
@Confluent
public final class ClassifiedException {

    /** Identifies important exceptions and allows to rewrite their messages. */
    private static final Map<CodeLocation, List<Handler>> classifiedExceptions;

    static {
        classifiedExceptions = new HashMap<>();

        // Add more classifications here:

        // Avoid duplicate causes for CREATE TABLE and ALTER TABLE
        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, ValidationException.class),
                Handler.forwardCauseOnly(
                        "Could not execute CreateTable", ExceptionClass.PLANNING_USER));
        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, TableException.class),
                Handler.forwardCauseOnly(
                        "Could not execute CreateTable", ExceptionClass.PLANNING_USER));
        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, ValidationException.class),
                Handler.forwardCauseOnly(
                        "Could not execute AlterTable", ExceptionClass.PLANNING_USER));
        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, TableException.class),
                Handler.forwardCauseOnly(
                        "Could not execute AlterTable", ExceptionClass.PLANNING_USER));

        // Don't delegate the user to options that can't be set.
        putClassifiedException(
                CodeLocation.inClass(CatalogSourceTable.class, ValidationException.class),
                Handler.customMessage(
                        "The 'OPTIONS' hint is allowed only when the config "
                                + "option 'table.dynamic-table-options.enabled' is set to true.",
                        ExceptionClass.PLANNING_USER,
                        "Cannot accept 'OPTIONS' hint. Please remove it from the query."));

        // Don't throw exceptions for operations we don't support
        final List<String> unsupportedOperations =
                Arrays.asList(
                        "convertDropTable",
                        "convertAlterTableCompact",
                        "convertCreateFunction",
                        "convertAlterFunction",
                        "convertDropFunction",
                        "convertBeginStatementSet",
                        "convertEndStatementSet",
                        "convertDropCatalog",
                        "convertCreateDatabase",
                        "convertDropDatabase",
                        "convertAlterDatabase",
                        "convertShowColumns",
                        "convertShowCreateTable",
                        "convertShowCreateView",
                        "convertDropView",
                        "convertShowViews",
                        "convertRichExplain",
                        "convertLoadModule",
                        "convertAddJar",
                        "convertRemoveJar",
                        "convertShowJars",
                        "convertUnloadModule",
                        "convertUseModules",
                        "convertShowModules",
                        "convertExecutePlan",
                        "convertCompilePlan",
                        "convertCompileAndExecutePlan",
                        "convertAnalyzeTable",
                        "convertDelete",
                        "convertUpdate");
        unsupportedOperations.forEach(
                op -> {
                    putClassifiedException(
                            CodeLocation.inMethod(
                                    SqlNodeToOperationConversion.class,
                                    op,
                                    ValidationException.class),
                            Handler.customMessage(
                                    ExceptionClass.PLANNING_USER,
                                    "The requested operation is not supported."));
                    putClassifiedException(
                            CodeLocation.inMethod(
                                    SqlNodeToOperationConversion.class, op, TableException.class),
                            Handler.customMessage(
                                    ExceptionClass.PLANNING_USER,
                                    "The requested operation is not supported."));
                });
    }

    static void putClassifiedException(CodeLocation codeLocation, Handler handler) {
        final List<Handler> handlerList =
                classifiedExceptions.computeIfAbsent(codeLocation, cl -> new ArrayList<>());
        handlerList.add(handler);
    }

    /** Available categories that can be assigned to exceptions. */
    public enum ExceptionClass {
        /** An exception that happened during planning and can be exposed to the user. */
        PLANNING_USER,

        /**
         * An exception that happened during planning and should be kept internal. As there is no
         * classification for it.
         */
        PLANNING_SYSTEM
    }

    /** Classifies the given exception into a {@link ExceptionClass} and message. */
    public static ClassifiedException of(Exception e, Set<Class<? extends Exception>> validCauses) {
        final StackTraceElement[] stackTrace = e.getStackTrace();
        if (stackTrace.length > 0) {
            final List<Handler> handlersByClass =
                    classifiedExceptions.get(
                            new CodeLocation(e.getClass(), stackTrace[0].getClassName(), null));
            if (handlersByClass != null) {
                final Optional<Handler> matchingHandler =
                        handlersByClass.stream().filter(h -> h.matches(e)).findFirst();
                if (matchingHandler.isPresent()) {
                    final Handler handler = matchingHandler.get();
                    return new ClassifiedException(
                            handler.exceptionClass, handler.messageProvider.apply(e, validCauses));
                }
            }
            final List<Handler> handlersByMethod =
                    classifiedExceptions.get(
                            new CodeLocation(
                                    e.getClass(),
                                    stackTrace[0].getClassName(),
                                    stackTrace[0].getMethodName()));
            if (handlersByMethod != null) {
                final Optional<Handler> matchingHandler =
                        handlersByMethod.stream().filter(h -> h.matches(e)).findFirst();
                if (matchingHandler.isPresent()) {
                    final Handler handler = matchingHandler.get();
                    return new ClassifiedException(
                            handler.exceptionClass, handler.messageProvider.apply(e, validCauses));
                }
            }
        }

        // If not specified with a custom rule above,
        // valid causes are always user exceptions for invalid statements.
        if (validCauses.contains(e.getClass())) {
            return new ClassifiedException(
                    ExceptionClass.PLANNING_USER, buildMessageWithCauses(e, validCauses));
        }

        return new ClassifiedException(
                ExceptionClass.PLANNING_SYSTEM, buildMessageWithCauses(e, null));
    }

    // --------------------------------------------------------------------------------------------

    private final ExceptionClass exceptionClass;

    private final String message;

    private ClassifiedException(ExceptionClass exceptionClass, String message) {
        this.exceptionClass = exceptionClass;
        this.message = message;
    }

    public ExceptionClass getExceptionClass() {
        return exceptionClass;
    }

    public String getMessage() {
        return message;
    }

    /** Defines which kind of exception needs to happen at which location (class and/or method). */
    private static class CodeLocation {
        private final Class<?> exceptionClass;
        private final String declaringClass;
        private final @Nullable String method;

        CodeLocation(Class<?> exceptionClass, String declaringClass, @Nullable String method) {
            this.exceptionClass = exceptionClass;
            this.declaringClass = declaringClass;
            this.method = method;
        }

        /** Code location defined as method. */
        static CodeLocation inMethod(
                Class<?> declaringClass, String method, Class<?> exceptionClass) {
            if (Stream.of(declaringClass.getDeclaredMethods())
                    .noneMatch(d -> d.getName().equals(method))) {
                throw new IllegalStateException(
                        String.format(
                                "Could not find method %s in %s. Code has changed?",
                                declaringClass.getName(), method));
            }
            return new CodeLocation(exceptionClass, declaringClass.getName(), method);
        }

        /** Code location defined as class. */
        static CodeLocation inClass(Class<?> declaringClass, Class<?> exceptionClass) {
            return new CodeLocation(exceptionClass, declaringClass.getName(), null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CodeLocation that = (CodeLocation) o;
            return exceptionClass.equals(that.exceptionClass)
                    && declaringClass.equals(that.declaringClass)
                    && Objects.equals(method, that.method);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exceptionClass, declaringClass, method);
        }
    }

    /** Handling logic for a matched exception. */
    private static class Handler {
        // handler only fires if message part matches
        private final @Nullable String messagePart;

        private final ExceptionClass exceptionClass;
        private final MessageProvider messageProvider;

        Handler(
                @Nullable String messagePart,
                ExceptionClass exceptionClass,
                MessageProvider messageProvider) {
            this.messagePart = messagePart;
            this.exceptionClass = exceptionClass;
            this.messageProvider = messageProvider;
        }

        boolean matches(Exception e) {
            if (messagePart == null) {
                return true;
            }
            return e.getMessage().contains(messagePart);
        }

        /** Classify and forward exception's message. */
        static Handler forwardMessage(ExceptionClass exceptionClass) {
            return forwardMessage(null, exceptionClass);
        }

        /** Classify and forward exception's message if message part matches. */
        static Handler forwardMessage(
                @Nullable String onMessagePart, ExceptionClass exceptionClass) {
            return new Handler(onMessagePart, exceptionClass, (e, validCauses) -> e.getMessage());
        }

        /** Classify and forward exception's message and all messages of causes. */
        static Handler forwardMessageWithCause(ExceptionClass exceptionClass) {
            return forwardMessageWithCause(null, exceptionClass);
        }

        /**
         * Classify and forward exception's message and all messages of causes if message part
         * matches.
         */
        static Handler forwardMessageWithCause(
                @Nullable String onMessagePart, ExceptionClass exceptionClass) {
            return new Handler(
                    onMessagePart, exceptionClass, ClassifiedException::buildMessageWithCauses);
        }

        /** Classify exception and rewrite error message. */
        static Handler customMessage(ExceptionClass exceptionClass, String message) {
            return customMessage(null, exceptionClass, message);
        }

        /** Classify exception and rewrite error message if message part matches. */
        static Handler customMessage(
                @Nullable String onMessagePart, ExceptionClass exceptionClass, String message) {
            return new Handler(onMessagePart, exceptionClass, (e, validCauses) -> message);
        }

        /** Classify exception and skip the top-level error message. */
        static Handler forwardCauseOnly(
                @Nullable String onMessagePart, ExceptionClass exceptionClass) {
            return new Handler(
                    onMessagePart,
                    exceptionClass,
                    (e, validCauses) -> {
                        if (e.getCause() instanceof Exception) {
                            return ClassifiedException.buildMessageWithCauses(
                                    (Exception) e.getCause(), validCauses);
                        }
                        return ClassifiedException.buildMessageWithCauses(e, validCauses);
                    });
        }
    }

    private interface MessageProvider {
        String apply(Exception e, Set<Class<? extends Exception>> validCauses);
    }

    /** Utility method to pretty print a chain of causes. */
    public static String buildMessageWithCauses(
            Exception e, @Nullable Set<Class<? extends Exception>> validCauses) {
        final StringBuilder builder = new StringBuilder();
        Exception currentException = e;
        while (currentException != null) {
            builder.append(currentException.getMessage());
            final Throwable cause = currentException.getCause();
            // Those exceptions should be safe to expose as causes
            if (cause != null && (validCauses == null || validCauses.contains(cause.getClass()))) {
                builder.append("\n\nCaused by: ");
                currentException = (Exception) cause;
            } else {
                currentException = null;
            }
        }
        return builder.toString();
    }
}
