/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.extraction;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.extraction.ConfluentUdfExtractor;
import org.apache.flink.table.types.extraction.ConfluentUdfExtractor.Signature;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.logging.Logger;

import static io.confluent.flink.udf.extractor.Extractor.ErrorCode.GENERAL_FAILED_EXTRACTION;
import static io.confluent.flink.udf.extractor.Extractor.ErrorCode.NOT_SCALAR_FUNCTION;
import static io.confluent.flink.udf.extractor.Extractor.ErrorCode.NO_PUBLIC_NO_ARG_CONSTRUCTOR;
import static io.confluent.flink.udf.extractor.Extractor.ErrorCode.TYPE_EXTRACTION_ERROR;
import static io.confluent.flink.udf.extractor.Extractor.ErrorCode.USER_CODE_NOT_FOUND;

/**
 * Contains all the logic required to take a class and extract the metadata about it that we want to
 * store about a UDF.
 */
public class MetadataExtractor {

    private static final Logger LOG = Logger.getLogger(MetadataExtractor.class.getName());

    /**
     * Extracts the metadata from a UDF class name.
     *
     * @param classLoader The classloader to use when loading the UDF class
     * @param className The name of the class to load
     * @return The extracted metadata
     * @throws MetadataExtractionException If anything went wrong during extraction, this is thrown
     */
    @SuppressWarnings("unchecked")
    public static Metadata extract(ClassLoader classLoader, String className)
            throws MetadataExtractionException {
        Class<? extends ScalarFunction> clazz;
        try {
            clazz = (Class<? extends ScalarFunction>) classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new MetadataExtractionException(USER_CODE_NOT_FOUND, "Can't find user class", e);
        } catch (ClassCastException e) {
            throw new MetadataExtractionException(
                    NOT_SCALAR_FUNCTION, "Class not a scalar function", e);
        }
        ScalarFunction instance;
        try {
            instance = clazz.getConstructor().newInstance();
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new MetadataExtractionException(
                    NO_PUBLIC_NO_ARG_CONSTRUCTOR, "Class needs a public constructor", e);
        } catch (InvocationTargetException e) {
            throw new MetadataExtractionException(
                    GENERAL_FAILED_EXTRACTION, "Couldn't instantiate constructor", e);
        }
        if (instance.getKind() != FunctionKind.SCALAR) {
            throw new MetadataExtractionException(
                    NOT_SCALAR_FUNCTION, "Class not a scalar function");
        }
        try {
            Method method = clazz.getMethod("getTypeInference", DataTypeFactory.class);
            if (!method.getDeclaringClass().equals(ScalarFunction.class)) {
                throw new MetadataExtractionException(
                        TYPE_EXTRACTION_ERROR,
                        "Custom type inference not supported. Use default ScalarFunction behavior.");
            }
        } catch (NoSuchMethodException e) {
            throw new MetadataExtractionException(
                    TYPE_EXTRACTION_ERROR,
                    "Internal error. getTypeInference(...) should always be defined");
        }
        ExtractorDataTypeFactory dataTypeFactory = new ExtractorDataTypeFactory(classLoader);
        List<Signature> signatures;
        try {
            signatures = ConfluentUdfExtractor.forScalarFunction(dataTypeFactory, clazz);
        } catch (ValidationException t) {
            throw new MetadataExtractionException(
                    TYPE_EXTRACTION_ERROR,
                    t.getCause() != null && t.getCause() instanceof ValidationException
                            ? t.getCause().getMessage()
                            : t.getMessage());
        }
        return new Metadata(signatures);
    }

    private MetadataExtractor() {}
}
