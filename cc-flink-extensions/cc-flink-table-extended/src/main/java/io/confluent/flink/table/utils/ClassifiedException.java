/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.utils;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.codegen.CodeGenException;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.operations.AlterSchemaConverter;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.util.OptionalUtils;
import org.apache.flink.util.Preconditions;

import io.confluent.flink.table.service.ServiceTasksOptions;
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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Utility for classifying exceptions and producing human-readable error messages.
 *
 * <p>Use {@link #getSensitiveMessage()} for reporting directly to the user.
 *
 * <p>Use {@link #getLogException()} and {@link #getLogMessage()} for logging.
 */
@Confluent
public final class ClassifiedException {

    /** Available categories that can be assigned to exceptions. */
    public enum ExceptionKind {
        /** An exception that can be exposed to the user. */
        USER,

        /** An exception that should be kept internal. As there is no classification for it. */
        INTERNAL
    }

    /** Initial list of causes known to be valid. */
    public static final Set<Class<? extends Throwable>> VALID_CAUSES =
            new HashSet<>(
                    Arrays.asList(
                            TableException.class,
                            ValidationException.class,
                            SqlValidateException.class,
                            DatabaseNotExistException.class,
                            TableNotExistException.class,
                            TableAlreadyExistException.class,
                            SqlValidatorException.class));

    private final ExceptionKind exceptionKind;
    private final String sensitiveMessage;
    private final @Nullable String logMessage;
    private final @Nullable Exception logException;

    private ClassifiedException(
            ExceptionKind exceptionKind,
            String sensitiveMessage,
            @Nullable String logMessage,
            @Nullable Exception logException) {
        this.exceptionKind = exceptionKind;
        this.sensitiveMessage = sensitiveMessage;
        this.logMessage = logMessage;
        this.logException = logException;
    }

    private ClassifiedException(ExceptionKind exceptionKind, String sensitiveMessage) {
        this(exceptionKind, sensitiveMessage, null, null);
    }

    public ExceptionKind getKind() {
        return exceptionKind;
    }

    /**
     * Message that can contain sensitive data. In case of {@link ExceptionKind#USER}) intended only
     * for the user. Should not be logged.
     */
    public String getSensitiveMessage() {
        return sensitiveMessage;
    }

    /** Message that is intended for logging. Highly sensitive data has been removed. */
    public String getLogMessage() {
        assert logMessage != null;
        return logMessage;
    }

    /** Rewritten exception that is intended for logging. Highly sensitive data has been removed. */
    public Exception getLogException() {
        assert logException != null;
        return logException;
    }

    /** Classifies the given exception. */
    public static ClassifiedException of(
            Throwable e,
            Set<Class<? extends Throwable>> validCauses,
            ReadableConfig publicOptions) {
        final StackTraceElement stackTrace = getCauseFromStackTrace(e.getStackTrace());

        final Optional<ClassifiedException> classifiedUserException =
                OptionalUtils.firstPresent(
                        // handler by declaring class
                        classifyBasedOnCodeLocation(
                                new CodeLocation(e.getClass(), stackTrace.getClassName(), null),
                                e,
                                validCauses,
                                publicOptions),
                        // handler by declaring class and method
                        () ->
                                classifyBasedOnCodeLocation(
                                        new CodeLocation(
                                                e.getClass(),
                                                stackTrace.getClassName(),
                                                stackTrace.getMethodName()),
                                        e,
                                        validCauses,
                                        publicOptions),
                        // handler by exception class
                        () ->
                                classifyBasedOnCodeLocation(
                                        new CodeLocation(e.getClass(), null, null),
                                        e,
                                        validCauses,
                                        publicOptions),
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

        // Decision what is exposed to the user
        final ClassifiedException nonLoggingException =
                classifiedUserException.orElseGet(() -> classifyAsSystemException(e));

        // Decision what is exposed to the logs
        return new ClassifiedException(
                nonLoggingException.exceptionKind,
                nonLoggingException.sensitiveMessage,
                cleanMessage(nonLoggingException.sensitiveMessage),
                CleanedException.of(e));
    }

    /** Utility method to pretty print a chain of causes. */
    public static Optional<String> buildMessageWithCauses(
            Throwable e, @Nullable Set<Class<? extends Throwable>> validCauses) {
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
            @Nullable Set<Class<? extends Throwable>> validCauses) {
        final List<String> messages = collectCauses(includeTopLevel, t, validCauses);
        return buildMessageWithCauses(messages);
    }

    // --------------------------------------------------------------------------------------------
    // Private helpers
    // --------------------------------------------------------------------------------------------

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

    private static final Pattern CATALOG_NOT_FOUND =
            Pattern.compile("A catalog with name \\[(.*)] does not exist\\.");

    static {
        classifiedExceptions = new HashMap<>();

        // Add more classifications here:

        // Avoid duplicate causes for CREATE TABLE and ALTER TABLE
        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, ValidationException.class),
                Handler.forwardCauseOnly("Could not execute CreateTable", ExceptionKind.USER));
        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, TableException.class),
                Handler.forwardCauseOnly("Could not execute CreateTable", ExceptionKind.USER));
        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, ValidationException.class),
                Handler.forwardCauseOnly("Could not execute AlterTable", ExceptionKind.USER));
        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, TableException.class),
                Handler.forwardCauseOnly("Could not execute AlterTable", ExceptionKind.USER));

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, TableException.class),
                Handler.rewriteMessage(
                        "in any of the catalogs",
                        ExceptionKind.USER,
                        message ->
                                message.substring(0, message.indexOf(" in any of the catalogs"))
                                        + "."));

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, CatalogException.class),
                Handler.forwardMessage("Current catalog has not been set.", ExceptionKind.USER));

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, CatalogException.class),
                Handler.rewriteMessage(
                        "database with name",
                        ExceptionKind.USER,
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
                        ExceptionKind.USER,
                        message -> {
                            final Matcher matcher = CATALOG_NOT_FOUND.matcher(message);
                            if (!matcher.matches()) {
                                throw new IllegalStateException(
                                        "The pattern should've been matched.");
                            }
                            final String catalogIdentifier = matcher.group(1);
                            return String.format(
                                    "A catalog with name or id '%s' cannot be resolved. Possible reasons:\n"
                                            + "\t1. You might not have permissions to access it.\n"
                                            + "\t2. The catalog might not exist.\n"
                                            + "\t3. There might be multiple catalogs with the same name.",
                                    catalogIdentifier);
                        }));

        // Remove all confluent specific table options
        putClassifiedException(
                CodeLocation.inClass(FactoryUtil.class, ValidationException.class),
                Handler.custom(
                        "Table options are:",
                        ExceptionKind.USER,
                        (e, validCauses, publicOptions) ->
                                buildMessageWithCauses(e, validCauses)
                                        .map(s -> s.replaceAll("'[^']*confluent[^']+'=.*\n", ""))));

        putClassifiedException(
                CodeLocation.inClass(ParserImpl.class, IllegalArgumentException.class),
                Handler.rewriteMessage(
                        "only single statement supported",
                        ExceptionKind.USER,
                        message ->
                                "Only a single statement is supported at a time. "
                                        + "Multiple INSERT INTO statements can be wrapped into a STATEMENT SET."));

        // Expose all exceptions from applying rules
        putClassifiedException(
                CodeLocation.inClass(VolcanoRuleCall.class, RuntimeException.class),
                Handler.forwardCauseOnly("Error occurred while applying rule", ExceptionKind.USER));
        putClassifiedException(
                CodeLocation.inClass(VolcanoRuleCall.class, RuntimeException.class),
                Handler.forwardCauseOnly("Error while applying rule", ExceptionKind.USER));

        // Don't delegate the user to options that can't be set.
        putClassifiedException(
                CodeLocation.inClass(CatalogSourceTable.class, ValidationException.class),
                Handler.rewriteMessage(
                        "The 'OPTIONS' hint is allowed only when the config "
                                + "option 'table.dynamic-table-options.enabled' is set to true.",
                        ExceptionKind.USER,
                        "Cannot accept 'OPTIONS' hint. Please remove it from the query."));

        // ADD WATERMARK is not supported
        putClassifiedException(
                CodeLocation.inClass(AlterSchemaConverter.class, ValidationException.class),
                Handler.rewriteMessage(
                        "The base table has already defined the watermark strategy",
                        ExceptionKind.USER,
                        "All tables declare a system-provided watermark by default. "
                                + "Use ALTER TABLE MODIFY for custom watermarks."));

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, ValidationException.class),
                Handler.rewriteMessage(
                        "A current catalog has not been set.",
                        ExceptionKind.USER,
                        msg ->
                                msg.replace(
                                        "A current catalog has not been set.",
                                        "A current, valid catalog has not been set.")));

        putClassifiedException(
                CodeLocation.inClass(CatalogManager.class, ValidationException.class),
                Handler.rewriteMessage(
                        "A current database has not been set.",
                        ExceptionKind.USER,
                        msg ->
                                msg.replace(
                                        "A current database has not been set.",
                                        "A current, valid database has not been set.")));

        putClassifiedException(
                CodeLocation.ofClass(CalciteContextException.class),
                Handler.custom(
                        null,
                        ExceptionKind.USER,
                        (e, validCauses, publicOptions) -> {
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
                                    ClassifiedException.of(
                                            (Exception) e.getCause(), validCauses, publicOptions);
                            if (classifiedCause.exceptionKind == ExceptionKind.USER) {
                                message.append("\n\nCaused by: ")
                                        .append(classifiedCause.sensitiveMessage);
                            }
                            return Optional.of(message.toString());
                        }));

        putClassifiedException(
                CodeLocation.ofClass(SqlValidatorException.class),
                Handler.rewriteMessageOnPattern(
                        TABLE_NOT_FOUND_PATTERN,
                        ExceptionKind.USER,
                        (msg, publicOptions) -> {
                            final Matcher matcher = TABLE_NOT_FOUND_PATTERN.matcher(msg);
                            if (!matcher.matches()) {
                                throw new IllegalStateException(
                                        "The pattern should've been matched.");
                            }
                            return String.format(
                                    "Table (or view) '%s' does not exist or you do not have permission to access it.\n"
                                            + "Using current catalog '%s' and current database '%s'.",
                                    matcher.group(1),
                                    publicOptions
                                            .getOptional(ServiceTasksOptions.SQL_CURRENT_CATALOG)
                                            .orElse(""),
                                    publicOptions
                                            .getOptional(ServiceTasksOptions.SQL_CURRENT_DATABASE)
                                            .orElse(""));
                        }));

        // Don't expose internal errors during failed validation
        putClassifiedException(
                CodeLocation.inClass(FlinkPlannerImpl.class, ValidationException.class),
                Handler.custom(
                        "SQL validation failed.",
                        ExceptionKind.USER,
                        (e, validCauses, publicOptions) -> {
                            if (!(e.getCause() instanceof Exception)) {
                                return Optional.empty();
                            }
                            final ClassifiedException classifiedCause =
                                    ClassifiedException.of(
                                            e.getCause(), validCauses, publicOptions);
                            if (classifiedCause.exceptionKind == ExceptionKind.USER) {
                                return Optional.of(classifiedCause.sensitiveMessage);
                            }
                            return Optional.empty();
                        }));

        // Codegen errors should not be valid causes,
        // but until the validation layer has improved significantly,
        // we need to allow it. The rule below matches for obvious bugs,
        // it has been manually tested.
        putClassifiedException(
                CodeLocation.ofClass(CodeGenException.class),
                Handler.custom(
                        null,
                        ExceptionKind.USER,
                        (e, validCauses, publicOptions) -> {
                            if (e.getMessage().contains("a bug")) {
                                return Optional.empty();
                            }
                            return Optional.of(e.getMessage());
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
                                    ExceptionKind.USER,
                                    "The requested operation is not supported."));
                    putClassifiedException(
                            CodeLocation.inMethod(
                                    SqlNodeToOperationConversion.class, op, TableException.class),
                            Handler.rewriteMessage(
                                    ExceptionKind.USER,
                                    "The requested operation is not supported."));
                });
    }

    private static void putClassifiedException(CodeLocation codeLocation, Handler handler) {
        final List<Handler> handlerList =
                classifiedExceptions.computeIfAbsent(codeLocation, cl -> new ArrayList<>());
        handlerList.add(handler);
    }

    private static Optional<ClassifiedException> classifyBasedOnCodeLocation(
            CodeLocation codeLocation,
            Throwable e,
            Set<Class<? extends Throwable>> validCauses,
            ReadableConfig publicOptions) {

        final List<Handler> possibleHandlers = classifiedExceptions.get(codeLocation);

        if (possibleHandlers == null) {
            return Optional.empty();
        }

        return possibleHandlers.stream()
                .filter(h -> h.matches(e))
                .findFirst()
                .map(
                        matchingHandler ->
                                classifyExceptionWithHandler(
                                        matchingHandler, e, validCauses, publicOptions));
    }

    private static ClassifiedException classifyExceptionWithHandler(
            Handler handler,
            Throwable e,
            Set<Class<? extends Throwable>> validCauses,
            ReadableConfig publicOptions) {
        final Optional<String> message =
                handler.messageProvider.apply(e, validCauses, publicOptions);
        return message.map(s -> new ClassifiedException(handler.exceptionKind, s))
                .orElseGet(() -> classifyAsSystemException(e));
    }

    private static ClassifiedException classifyAsSystemException(Throwable e) {
        final String systemMessage =
                buildMessageWithCauses(IncludeTopLevel.ALWAYS, e, null)
                        .orElse(e.getClass().getName());
        return new ClassifiedException(ExceptionKind.INTERNAL, systemMessage);
    }

    private static ClassifiedException classifyAsUserExceptionIfPossible(
            Throwable e, Set<Class<? extends Throwable>> validCauses) {
        final Optional<String> message = buildMessageWithCauses(e, validCauses);
        return message.map(s -> new ClassifiedException(ExceptionKind.USER, s))
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

        private final ExceptionKind exceptionKind;
        private final MessageProvider messageProvider;

        Handler(
                @Nullable String messagePart,
                @Nullable Pattern messageRegex,
                ExceptionKind exceptionKind,
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
            this.exceptionKind = exceptionKind;
            this.messageProvider = messageProvider;
        }

        boolean matches(Throwable e) {
            return messagePredicate.test(e.getMessage());
        }

        /** Classify and forward exception's message if message part matches. */
        static Handler forwardMessage(@Nullable String onMessagePart, ExceptionKind exceptionKind) {
            return new Handler(
                    onMessagePart,
                    null,
                    exceptionKind,
                    (e, validCauses, publicOptions) -> Optional.of(e.getMessage()));
        }

        /** Classify exception and rewrite error message. */
        static Handler rewriteMessage(ExceptionKind exceptionKind, String message) {
            return rewriteMessage(null, exceptionKind, message);
        }

        /** Classify exception and rewrite error message if message part matches. */
        static Handler rewriteMessage(
                @Nullable String onMessagePart, ExceptionKind exceptionKind, String message) {
            return new Handler(
                    onMessagePart,
                    null,
                    exceptionKind,
                    (e, validCauses, publicOptions) -> Optional.of(message));
        }

        /** Classify exception and rewrite error message using a function. */
        static Handler rewriteMessage(
                @Nullable String onMessagePart,
                ExceptionKind exceptionKind,
                Function<String, String> rewriteMessage) {
            return new Handler(
                    onMessagePart,
                    null,
                    exceptionKind,
                    (e, validCauses, publicOptions) -> {
                        final String originalMessage = e.getMessage();
                        return Optional.of(rewriteMessage.apply(originalMessage));
                    });
        }

        static Handler rewriteMessage(
                @Nullable String onMessagePart,
                ExceptionKind exceptionKind,
                BiFunction<String, ReadableConfig, String> rewriteMessage) {
            return new Handler(
                    onMessagePart,
                    null,
                    exceptionKind,
                    (e, validCauses, publicOptions) -> {
                        final String originalMessage = e.getMessage();
                        return Optional.of(rewriteMessage.apply(originalMessage, publicOptions));
                    });
        }

        /** Classify exception and rewrite error message using a function. */
        static Handler rewriteMessageOnPattern(
                @Nullable Pattern onMessageRegex,
                ExceptionKind exceptionKind,
                BiFunction<String, ReadableConfig, String> rewriteMessage) {
            return new Handler(
                    null,
                    onMessageRegex,
                    exceptionKind,
                    (e, validCauses, publicOptions) ->
                            Optional.of(rewriteMessage.apply(e.getMessage(), publicOptions)));
        }

        /** Classify exception and skip the top-level error message. */
        static Handler forwardCauseOnly(
                @Nullable String onMessagePart, ExceptionKind exceptionKind) {
            return new Handler(
                    onMessagePart,
                    null,
                    exceptionKind,
                    (e, validCauses, publicOptions) -> {
                        if (e.getCause() instanceof Exception) {
                            return ClassifiedException.buildMessageWithCauses(
                                    e.getCause(), validCauses);
                        }
                        return ClassifiedException.buildMessageWithCauses(e, validCauses);
                    });
        }

        /** Completely custom message. */
        static Handler custom(
                @Nullable String onMessagePart,
                ExceptionKind exceptionKind,
                MessageProvider provider) {
            return new Handler(onMessagePart, null, exceptionKind, provider);
        }
    }

    private interface MessageProvider {

        Optional<String> apply(
                Throwable e,
                Set<Class<? extends Throwable>> validCauses,
                ReadableConfig publicOptions);
    }

    private static List<String> collectCauses(
            IncludeTopLevel includeTopLevel,
            Throwable t,
            @Nullable Set<Class<? extends Throwable>> validCauses) {

        final List<String> messages = new ArrayList<>();
        if (includeTopLevel == IncludeTopLevel.ALWAYS) {
            messages.add(t.getMessage());
        } else if (includeTopLevel == IncludeTopLevel.IF_VALID_CAUSE) {
            if (validCauses == null || validCauses.contains(t.getClass())) {
                messages.add(t.getMessage());
            } else {
                return Collections.emptyList();
            }
        }
        while (t != null) {
            final Throwable cause = t.getCause();
            if (cause != null && (validCauses == null || validCauses.contains(cause.getClass()))) {
                t = cause;
                messages.add(t.getMessage());
            } else {
                t = null;
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

    private static final String REMOVED = "<<removed>>";

    static String cleanMessage(@Nullable String userMessage) {
        if (userMessage == null) {
            return "";
        }
        // Remove literals (i.e. enclosed in single quotes such as "'Bob''s literal'"
        return userMessage.replaceAll("'([^']*'')*[^']*'", String.format("'%s'", REMOVED));
    }
}
