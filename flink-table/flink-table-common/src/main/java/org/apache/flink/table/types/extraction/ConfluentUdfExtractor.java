/*
 * Copyright 2024 Confluent Inc.
 */

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.extraction.BaseMappingExtractor.createParameterSignatureExtraction;
import static org.apache.flink.table.types.extraction.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createParameterAndReturnTypeVerification;
import static org.apache.flink.table.types.extraction.FunctionMappingExtractor.createReturnTypeResultExtraction;

/**
 * Similar to {@link TypeInferenceExtractor}, but extracts UDF signatures rather than a {@link
 * org.apache.flink.table.types.inference.TypeInference} object.
 */
@Confluent
public class ConfluentUdfExtractor {

    /** Extracts a type inference from a {@link ScalarFunction}. */
    public static List<Signature> forScalarFunction(
            DataTypeFactory typeFactory, Class<? extends ScalarFunction> function) {
        final FunctionMappingExtractor mappingExtractor =
                new FunctionMappingExtractor(
                        typeFactory,
                        function,
                        UserDefinedFunctionHelper.SCALAR_EVAL,
                        createParameterSignatureExtraction(0),
                        null,
                        createReturnTypeResultExtraction(),
                        createParameterAndReturnTypeVerification());
        return extractTypeInference(mappingExtractor, ConfluentUdfExtractor::extractSignatures);
    }

    private static List<Signature> extractTypeInference(
            FunctionMappingExtractor mappingExtractor,
            Function<FunctionMappingExtractor, List<Signature>> signatureFactory) {
        try {
            return signatureFactory.apply(mappingExtractor);
        } catch (Throwable t) {
            throw extractionError(
                    t,
                    "Could not extract a valid types for function class '%s'. "
                            + "Please check for implementation mistakes and/or provide a corresponding hint.",
                    mappingExtractor.getFunction().getName());
        }
    }

    private static List<Signature> extractSignatures(FunctionMappingExtractor mappingExtractor) {
        final Map<FunctionSignatureTemplate, FunctionResultTemplate> outputMapping =
                mappingExtractor.extractOutputMapping();

        List<Signature> signatures = new ArrayList<>();
        for (FunctionSignatureTemplate signatureTemplate : outputMapping.keySet()) {
            FunctionResultTemplate resultTemplate = outputMapping.get(signatureTemplate);
            if (signatureTemplate.isVarArgs) {
                throw new ValidationException("Varargs are not allowed");
            }
            List<DataType> argDataTypes =
                    signatureTemplate.argumentTemplates.stream()
                            .map(
                                    arg -> {
                                        if (arg.dataType == null) {
                                            throw new ValidationException(
                                                    "Only explicit types allowed");
                                        }
                                        return arg.dataType;
                                    })
                            .collect(Collectors.toList());
            DataType resultDataType = resultTemplate.dataType;
            signatures.add(new Signature(argDataTypes, resultDataType));
        }
        return signatures;
    }

    /** Represents the signature of a UDF method. */
    public static class Signature {
        private final List<DataType> argumentTypes;
        private final List<String> serializedArgumentTypes;
        private final DataType returnType;
        private final String serializedReturnType;

        public Signature(List<DataType> argumentTypes, DataType returnType) {
            this.argumentTypes = argumentTypes;
            this.returnType = returnType;
            this.serializedArgumentTypes =
                    argumentTypes.stream()
                            .map(dataType -> dataType.getLogicalType().asSerializableString())
                            .collect(Collectors.toList());
            this.serializedReturnType = returnType.getLogicalType().asSerializableString();
        }

        public List<String> getSerializedArgumentTypes() {
            return serializedArgumentTypes;
        }

        public String getSerializedReturnType() {
            return serializedReturnType;
        }

        public List<DataType> getArgumentTypes() {
            return argumentTypes;
        }

        public DataType getReturnType() {
            return returnType;
        }
    }
}
