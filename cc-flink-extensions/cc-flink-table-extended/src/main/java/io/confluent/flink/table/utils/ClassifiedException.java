/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.operations.AlterSchemaConverter;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.util.OptionalUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.volcano.VolcanoRuleCall;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteResource;
import org.apache.calcite.sql.validate.SqlValidatorException;

import javax.annotation.Nullable;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Classified exception with a message that can be exposed to the user. */
@Confluent
public final class ClassifiedException {

    /** Initial list of causes known to be valid. */
    public static final Set<Class<? extends Exception>> VALID_CAUSES =
            new HashSet<>(
                    Arrays.asList(
                            TableException.class,
                            ValidationException.class,
                            SqlValidateException.class,
                            DatabaseNotExistException.class,
                            TableNotExistException.class,
                            TableAlreadyExistException.class,
                            SqlValidatorException.class));

    /** Identifies important exceptions and allows to rewrite their messages. */
    private static final Map<CodeLocation, List<Handler>> classifiedExceptions;

    /**
     * Pattern to find an exception for a Table not found. This should match for:
     *
     * <ul>
     *   <li>{@link CalciteResource#objectNotFound(String)}
     *   <li>{@link CalciteResource#objectNotFoundWithin(String, String)}
     *   <li>{@link CalciteResource#objectNotFoundDidYouMean(String, String)}
     *   <li>{@link CalciteResource#objectNotFoundWithinDidYouMean(String, String, String)}
     * </ul>
     */
    private static final Pattern TABLE_NOT_FOUND_PATTERN =
            Pattern.compile("Object '(.*)' not found.*");

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

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, TableException.class),
                Handler.rewriteMessage(
                        "in any of the catalogs",
                        ExceptionClass.PLANNING_USER,
                        message ->
                                message.substring(0, message.indexOf(" in any of the catalogs"))
                                        + "."));

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, CatalogException.class),
                Handler.forwardMessage(
                        "Current catalog has not been set.", ExceptionClass.PLANNING_USER));

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, CatalogException.class),
                Handler.rewriteMessage(
                        "database with name",
                        ExceptionClass.PLANNING_USER,
                        message ->
                                // from:
                                // "A database with name [%s] does not exist in the catalog: [%s]."
                                // to:
                                // "A database with name '%s' does not exist, or you have no
                                // permissions to access it in catalog '%s'."
                                message.replace(
                                                " in the catalog:",
                                                ", or you have no permissions to access it in catalog")
                                        .replace('[', '\'')
                                        .replace(']', '\'')));

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, CatalogException.class),
                Handler.rewriteMessage(
                        "catalog with name",
                        ExceptionClass.PLANNING_USER,
                        message ->
                                // from:
                                // "A catalog with name [%s] does not exist."
                                // to:
                                // "A catalog with name '%s' does not exist, or you have no
                                // permissions to access it."
                                message.replace(
                                                "does not exist",
                                                "does not exist, or you have no permissions to access it")
                                        .replace('[', '\'')
                                        .replace(']', '\'')));

        putClassifiedException(
                CodeLocation.inClass(ParserImpl.class, IllegalArgumentException.class),
                Handler.rewriteMessage(
                        "only single statement supported",
                        ExceptionClass.PLANNING_USER,
                        message ->
                                "Only a single statement is supported at a time. "
                                        + "Multiple INSERT INTO statements can be wrapped into a STATEMENT SET."));

        // Expose all exceptions from applying rules
        putClassifiedException(
                CodeLocation.inClass(VolcanoRuleCall.class, RuntimeException.class),
                Handler.forwardCauseOnly(
                        "Error occurred while applying rule", ExceptionClass.PLANNING_USER));
        putClassifiedException(
                CodeLocation.inClass(VolcanoRuleCall.class, RuntimeException.class),
                Handler.forwardCauseOnly(
                        "Error while applying rule", ExceptionClass.PLANNING_USER));

        // Don't delegate the user to options that can't be set.
        putClassifiedException(
                CodeLocation.inClass(CatalogSourceTable.class, ValidationException.class),
                Handler.rewriteMessage(
                        "The 'OPTIONS' hint is allowed only when the config "
                                + "option 'table.dynamic-table-options.enabled' is set to true.",
                        ExceptionClass.PLANNING_USER,
                        "Cannot accept 'OPTIONS' hint. Please remove it from the query."));

        // ADD WATERMARK is not supported
        putClassifiedException(
                CodeLocation.inClass(AlterSchemaConverter.class, ValidationException.class),
                Handler.rewriteMessage(
                        "The base table has already defined the watermark strategy",
                        ExceptionClass.PLANNING_USER,
                        "All tables declare a system-provided watermark by default. "
                                + "Use ALTER TABLE MODIFY for custom watermarks."));

        putClassifiedException(
                CodeLocation.ofClass(CalciteContextException.class),
                Handler.custom(
                        null,
                        ExceptionClass.PLANNING_USER,
                        (e, validCauses) -> {
                            final StringBuilder message = new StringBuilder();
                            message.append("SQL validation failed. ");
                            final CalciteContextException context = (CalciteContextException) e;
                            if (context.getPosLine() == context.getEndPosLine()
                                    && context.getPosColumn() == context.getEndPosColumn()) {
                                message.append(
                                        String.format(
                                                "Error at or near line %s, column %s.",
                                                context.getPosLine(), context.getPosColumn()));
                            } else {
                                message.append(
                                        String.format(
                                                "Error from line %s, column %s to line %s, column %s.",
                                                context.getPosLine(),
                                                context.getPosColumn(),
                                                context.getEndPosLine(),
                                                context.getEndPosColumn()));
                            }

                            if (!(e.getCause() instanceof Exception)) {
                                return Optional.of(message.toString());
                            }

                            final ClassifiedException classifiedCause =
                                    ClassifiedException.of((Exception) e.getCause(), validCauses);
                            if (classifiedCause.exceptionClass == ExceptionClass.PLANNING_USER) {
                                message.append("\n\nCaused by: ").append(classifiedCause.message);
                            }
                            return Optional.of(message.toString());
                        }));

        putClassifiedException(
                CodeLocation.ofClass(SqlValidatorException.class),
                Handler.rewriteMessageFromException(
                        TABLE_NOT_FOUND_PATTERN,
                        ExceptionClass.PLANNING_USER,
                        ex -> {
                            final Matcher matcher =
                                    TABLE_NOT_FOUND_PATTERN.matcher(ex.getMessage());
                            if (!matcher.matches()) {
                                throw new IllegalStateException(
                                        "The pattern should've been matched.");
                            }
                            return String.format(
                                    "Table (or view) '%s' does not exist or you do not have permission to access it.",
                                    matcher.group(1));
                        }));

        // Don't expose internal errors during failed validation
        putClassifiedException(
                CodeLocation.inClass(FlinkPlannerImpl.class, ValidationException.class),
                Handler.custom(
                        "SQL validation failed.",
                        ExceptionClass.PLANNING_USER,
                        (e, validCauses) -> {
                            if (!(e.getCause() instanceof Exception)) {
                                return Optional.empty();
                            }
                            final ClassifiedException classifiedCause =
                                    ClassifiedException.of((Exception) e.getCause(), validCauses);
                            if (classifiedCause.exceptionClass == ExceptionClass.PLANNING_USER) {
                                return Optional.of(classifiedCause.message);
                            }
                            return Optional.empty();
                        }));

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
                            Handler.rewriteMessage(
                                    ExceptionClass.PLANNING_USER,
                                    "The requested operation is not supported."));
                    putClassifiedException(
                            CodeLocation.inMethod(
                                    SqlNodeToOperationConversion.class, op, TableException.class),
                            Handler.rewriteMessage(
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
        final StackTraceElement stackTrace = getCauseFromStackTrace(e.getStackTrace());

        final Optional<ClassifiedException> classifiedUserException =
                OptionalUtils.firstPresent(
                        // handler by declaring class
                        classifyBasedOnCodeLocation(
                                new CodeLocation(e.getClass(), stackTrace.getClassName(), null),
                                e,
                                validCauses),
                        // handler by declaring class and method
                        () ->
                                classifyBasedOnCodeLocation(
                                        new CodeLocation(
                                                e.getClass(),
                                                stackTrace.getClassName(),
                                                stackTrace.getMethodName()),
                                        e,
                                        validCauses),
                        // handler by exception class
                        () ->
                                classifyBasedOnCodeLocation(
                                        new CodeLocation(e.getClass(), null, null), e, validCauses),
                        () -> {
                            // If not specified with a custom rule above,
                            // valid causes are always user exceptions for invalid statements.
                            if (validCauses.contains(e.getClass())) {
                                return Optional.of(
                                        classifyAsUserExceptionIfPossible(e, validCauses));
                            } else {
                                return Optional.empty();
                            }
                        });

        return classifiedUserException.orElseGet(() -> classifyAsSystemException(e));
    }

    private static Optional<ClassifiedException> classifyBasedOnCodeLocation(
            CodeLocation codeLocation, Exception e, Set<Class<? extends Exception>> validCauses) {

        final List<Handler> possibleHandlers = classifiedExceptions.get(codeLocation);

        if (possibleHandlers == null) {
            return Optional.empty();
        }

        return possibleHandlers.stream()
                .filter(h -> h.matches(e))
                .findFirst()
                .map(
                        matchingHandler ->
                                classifyExceptionWithHandler(matchingHandler, e, validCauses));
    }

    private static ClassifiedException classifyExceptionWithHandler(
            Handler handler, Exception e, Set<Class<? extends Exception>> validCauses) {
        final Optional<String> message = handler.messageProvider.apply(e, validCauses);
        return message.map(s -> new ClassifiedException(handler.exceptionClass, s))
                .orElseGet(() -> classifyAsSystemException(e));
    }

    private static ClassifiedException classifyAsSystemException(Exception e) {
        final String systemMessage =
                buildMessageWithCauses(IncludeTopLevel.ALWAYS, e, null)
                        .orElse(e.getClass().getName());
        return new ClassifiedException(ExceptionClass.PLANNING_SYSTEM, systemMessage);
    }

    private static ClassifiedException classifyAsUserExceptionIfPossible(
            Exception e, Set<Class<? extends Exception>> validCauses) {
        final Optional<String> message = buildMessageWithCauses(e, validCauses);
        return message.map(s -> new ClassifiedException(ExceptionClass.PLANNING_USER, s))
                .orElseGet(() -> classifyAsSystemException(e));
    }

    private static StackTraceElement getCauseFromStackTrace(StackTraceElement[] stackTrace) {
        // Best effort to find the first public and useful stack trace element
        for (final StackTraceElement current : stackTrace) {
            try {
                if (current.getClassName().equals(Preconditions.class.getName())) {
                    continue;
                }

                final Class<?> c = Class.forName(current.getClassName());
                if (Modifier.isPublic(c.getModifiers())) {
                    return current;
                }
            } catch (Throwable t) {
                return stackTrace[0];
            }
        }

        return stackTrace[0];
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
        private final @Nullable String declaringClass;
        private final @Nullable String method;

        CodeLocation(
                Class<?> exceptionClass, @Nullable String declaringClass, @Nullable String method) {
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

        static CodeLocation ofClass(Class<?> exceptionClass) {
            return new CodeLocation(exceptionClass, null, null);
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
                    && Objects.equals(declaringClass, that.declaringClass)
                    && Objects.equals(method, that.method);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exceptionClass, declaringClass, method);
        }
    }

    /** Handling logic for a matched exception. */
    private static class Handler {

        // handler only fires if message part matches or the message matches the pattern
        private final Predicate<String> messagePredicate;

        private final ExceptionClass exceptionClass;
        private final MessageProvider messageProvider;

        Handler(
                @Nullable String messagePart,
                @Nullable Pattern messageRegex,
                ExceptionClass exceptionClass,
                MessageProvider messageProvider) {
            Preconditions.checkArgument(
                    messagePart == null || messageRegex == null,
                    "Can not use pattern and string matching at the same time.");
            this.messagePredicate =
                    OptionalUtils.firstPresent(
                                    Optional.ofNullable(messagePart)
                                            .map(expected -> message -> message.contains(expected)),
                                    () ->
                                            Optional.ofNullable(messageRegex)
                                                    .map(Pattern::asPredicate))
                            .orElse(message -> true);
            this.exceptionClass = exceptionClass;
            this.messageProvider = messageProvider;
        }

        boolean matches(Exception e) {
            return messagePredicate.test(e.getMessage());
        }

        /** Classify and forward exception's message if message part matches. */
        static Handler forwardMessage(
                @Nullable String onMessagePart, ExceptionClass exceptionClass) {
            return new Handler(
                    onMessagePart,
                    null,
                    exceptionClass,
                    (e, validCauses) -> Optional.of(e.getMessage()));
        }

        /** Classify exception and rewrite error message. */
        static Handler rewriteMessage(ExceptionClass exceptionClass, String message) {
            return rewriteMessage(null, exceptionClass, message);
        }

        /** Classify exception and rewrite error message if message part matches. */
        static Handler rewriteMessage(
                @Nullable String onMessagePart, ExceptionClass exceptionClass, String message) {
            return new Handler(
                    onMessagePart, null, exceptionClass, (e, validCauses) -> Optional.of(message));
        }

        /** Classify exception and rewrite error message using a function. */
        static Handler rewriteMessage(
                @Nullable String onMessagePart,
                ExceptionClass exceptionClass,
                Function<String, String> rewriteMessage) {
            return new Handler(
                    onMessagePart,
                    null,
                    exceptionClass,
                    (e, validCauses) -> {
                        final String originalMessage = e.getMessage();
                        return Optional.of(rewriteMessage.apply(originalMessage));
                    });
        }

        /** Classify exception and rewrite error message using a function. */
        static Handler rewriteMessageFromException(
                @Nullable Pattern onMessageRegex,
                ExceptionClass exceptionClass,
                Function<Exception, String> rewriteMessage) {
            return new Handler(
                    null,
                    onMessageRegex,
                    exceptionClass,
                    (e, validCauses) -> Optional.of(rewriteMessage.apply(e)));
        }

        /** Classify exception and skip the top-level error message. */
        static Handler forwardCauseOnly(
                @Nullable String onMessagePart, ExceptionClass exceptionClass) {
            return new Handler(
                    onMessagePart,
                    null,
                    exceptionClass,
                    (e, validCauses) -> {
                        if (e.getCause() instanceof Exception) {
                            return ClassifiedException.buildMessageWithCauses(
                                    (Exception) e.getCause(), validCauses);
                        }
                        return ClassifiedException.buildMessageWithCauses(e, validCauses);
                    });
        }

        /** Completely custom message. */
        static Handler custom(
                @Nullable String onMessagePart,
                ExceptionClass exceptionClass,
                MessageProvider provider) {
            return new Handler(onMessagePart, null, exceptionClass, provider);
        }
    }

    private interface MessageProvider {

        Optional<String> apply(Exception e, Set<Class<? extends Exception>> validCauses);
    }

    private static List<String> collectCauses(
            IncludeTopLevel includeTopLevel,
            Throwable t,
            @Nullable Set<Class<? extends Exception>> validCauses) {
        if (!(t instanceof Exception)) {
            return Collections.emptyList();
        }
        Exception e = (Exception) t;

        final List<String> messages = new ArrayList<>();
        if (includeTopLevel == IncludeTopLevel.ALWAYS) {
            messages.add(e.getMessage());
        } else if (includeTopLevel == IncludeTopLevel.IF_VALID_CAUSE) {
            if (validCauses == null || validCauses.contains(e.getClass())) {
                messages.add(e.getMessage());
            } else {
                return Collections.emptyList();
            }
        }
        while (e != null) {
            final Throwable cause = e.getCause();
            if (cause != null && (validCauses == null || validCauses.contains(cause.getClass()))) {
                e = (Exception) cause;
                messages.add(e.getMessage());
            } else {
                e = null;
            }
        }

        return messages;
    }

    private static Optional<String> buildMessageWithCauses(List<String> messages) {
        if (messages.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(String.join("\n\nCaused by: ", messages));
    }

    /** Utility method to pretty print a chain of causes. */
    public static Optional<String> buildMessageWithCauses(
            Exception e, @Nullable Set<Class<? extends Exception>> validCauses) {
        return buildMessageWithCauses(IncludeTopLevel.IF_VALID_CAUSE, e, validCauses);
    }

    /** Parameters for the cause collection. */
    public enum IncludeTopLevel {

        /**
         * We ignore the top level exception, we do not use its message nor check it against valid
         * causes, but proceed with its causes.
         */
        IGNORE,

        /**
         * If the top level exception is on the valid cause list its message is included, and we
         * proceed with its causes, otherwise we stop processing the exception.
         */
        IF_VALID_CAUSE,

        /**
         * We always include the top level exception message and proceed with checking its causes.
         */
        ALWAYS,
    }

    /** Utility method to pretty print a chain of causes. */
    public static Optional<String> buildMessageWithCauses(
            IncludeTopLevel includeTopLevel,
            Throwable t,
            @Nullable Set<Class<? extends Exception>> validCauses) {
        final List<String> messages = collectCauses(includeTopLevel, t, validCauses);
        return buildMessageWithCauses(messages);
    }
}
