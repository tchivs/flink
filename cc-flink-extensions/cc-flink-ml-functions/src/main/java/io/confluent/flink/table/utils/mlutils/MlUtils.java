/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.utils.mlutils;

import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.compute.credentials.InMemoryCredentialDecrypterImpl;
import io.confluent.flink.credentials.CredentialDecrypter;
import okhttp3.MediaType;
import okhttp3.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

/** Utilities for ML module. */
public class MlUtils {

    public static AbstractDataType getAbstractType(final UnresolvedColumn column) {
        if (column instanceof UnresolvedPhysicalColumn) {
            return ((UnresolvedPhysicalColumn) column).getDataType();
        } else if (column instanceof UnresolvedMetadataColumn) {
            return ((UnresolvedMetadataColumn) column).getDataType();
        } else {
            throw new IllegalArgumentException(
                    "Only UnresolvedPhysicalColumn and UnresolvedMetaColumn supported");
        }
    }

    /**
     * Get logical type from UnresolvedColumn.
     *
     * @param column Unresolved column
     * @return parsed logical type
     */
    public static LogicalType getLogicalType(final UnresolvedColumn column) {
        AbstractDataType abstractDataType = getAbstractType(column);

        if (abstractDataType instanceof DataType) {
            return ((DataType) abstractDataType).getLogicalType();
        } else if (abstractDataType instanceof UnresolvedDataType) {
            final String formatedTypeText = abstractDataType.toString();

            // toString of UnresolvedDataType returns "[%s]" formatted string
            final String typeText = formatedTypeText.substring(1, formatedTypeText.length() - 1);
            return LogicalTypeParser.parse(
                    typeText, Thread.currentThread().getContextClassLoader());
        }
        throw new IllegalArgumentException(
                "Not supported abstractDataType: "
                        + abstractDataType.getClass().getSuperclass().getSimpleName());
    }

    /** Get the bottommost leaf type from a nested type. */
    public static LogicalType getLeafType(LogicalType type) {
        while (type.getTypeRoot() == LogicalTypeRoot.ARRAY) {
            type = type.getChildren().get(0);
        }
        return type;
    }

    /** Get the response string from an http response. */
    public static String getResponseString(Response response) {
        try {
            return response.body().string();
        } catch (IOException e) {
            throw new FlinkRuntimeException("Error reading ML Predict response body to string", e);
        }
    }

    /** Get the response bytes from an http response. */
    public static byte[] getResponseBytes(Response response) {
        try {
            return response.body().bytes();
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Error reading ML Predict response body to byte array", e);
        }
    }

    /** Make an http response from a string. */
    public static Response makeResponse(String responseString) {
        return new Response.Builder()
                .code(200)
                .message("OK")
                .protocol(okhttp3.Protocol.HTTP_1_1)
                .request(new okhttp3.Request.Builder().url("http://localhost").build())
                .body(okhttp3.ResponseBody.create(null, responseString))
                .build();
    }

    /** Make an http response from bytes with headers. */
    public static Response makeResponse(byte[] responseString, okhttp3.Headers headers) {
        MediaType mediaType = null;
        if (headers != null && headers.get("Content-Type") != null) {
            mediaType = MediaType.parse(headers.get("Content-Type"));
        }
        return new Response.Builder()
                .code(200)
                .message("OK")
                .protocol(okhttp3.Protocol.HTTP_1_1)
                .request(new okhttp3.Request.Builder().url("http://localhost").build())
                .headers(headers)
                .body(okhttp3.ResponseBody.create(mediaType, responseString))
                .build();
    }

    /** Get a Triton Tensor DataType String from a logical type. */
    public static String getTritonDataType(LogicalType type) {
        LogicalTypeRoot typeRoot = type.getTypeRoot();
        while (typeRoot == LogicalTypeRoot.ARRAY) {
            // Keep unnesting arrays until we find the inner type.
            type = type.getChildren().get(0);
            typeRoot = type.getTypeRoot();
        }
        switch (typeRoot) {
            case BOOLEAN:
                return "BOOL";
            case TINYINT:
                return "INT8";
            case SMALLINT:
                return "INT16";
            case INTEGER:
                return "INT32";
            case BIGINT:
                return "INT64";
            case FLOAT:
                return "FP32";
            case DOUBLE:
                return "FP64";
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return "BYTES";
            default:
                throw new IllegalArgumentException(
                        "Unsupported type for tensor: " + type.getTypeRoot());
        }
    }

    /** Check a logical type against a Triton Tensor DataType String. */
    public static Boolean isAcceptableTritonDataType(LogicalType logicalType, String dataType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        while (typeRoot == LogicalTypeRoot.ARRAY) {
            // Keep unnesting arrays until we find the inner type.
            logicalType = logicalType.getChildren().get(0);
            typeRoot = logicalType.getTypeRoot();
        }
        switch (dataType) {
            case "BOOL":
                return typeRoot == LogicalTypeRoot.BOOLEAN;
            case "INT8":
                return typeRoot == LogicalTypeRoot.TINYINT
                        || typeRoot == LogicalTypeRoot.SMALLINT
                        || typeRoot == LogicalTypeRoot.INTEGER
                        || typeRoot == LogicalTypeRoot.BIGINT;
            case "UINT8":
            case "INT16":
                return typeRoot == LogicalTypeRoot.SMALLINT
                        || typeRoot == LogicalTypeRoot.INTEGER
                        || typeRoot == LogicalTypeRoot.BIGINT;
            case "UINT16":
            case "INT32":
                return typeRoot == LogicalTypeRoot.INTEGER || typeRoot == LogicalTypeRoot.BIGINT;
            case "UINT32":
            case "UINT64": // We'll have to check for overflow when converting UINT64 to BigInt.
            case "INT64":
                return typeRoot == LogicalTypeRoot.BIGINT;
            case "FP16":
            case "FP32":
            case "BF16": // BF16 is bfloat16, a.k.a. brain floating point.
                return typeRoot == LogicalTypeRoot.FLOAT || typeRoot == LogicalTypeRoot.DOUBLE;
            case "FP64":
                return typeRoot == LogicalTypeRoot.DOUBLE;
            case "BYTES":
                return typeRoot == LogicalTypeRoot.CHAR
                        || typeRoot == LogicalTypeRoot.VARCHAR
                        || typeRoot == LogicalTypeRoot.BINARY
                        || typeRoot == LogicalTypeRoot.VARBINARY;
            default:
                throw new IllegalArgumentException("Unsupported Tensor data type: " + dataType);
        }
    }

    /** Check if a logical type is compatible with a given tensor shape. */
    public static Boolean isShapeCompatible(LogicalType logicalType, List<Integer> shape) {
        if (logicalType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
            if (shape.size() == 0) {
                return false;
            }
            LogicalType child = logicalType.getChildren().get(0);
            return isShapeCompatible(child, shape.subList(1, shape.size()));
        } else if (logicalType.getTypeRoot() == LogicalTypeRoot.CHAR) {
            CharType charType = (CharType) logicalType;
            return shape.size() == 1 && shape.get(0) == charType.getLength();
        } else if (logicalType.getTypeRoot() == LogicalTypeRoot.BINARY) {
            BinaryType binaryType = (BinaryType) logicalType;
            return shape.size() == 1 && shape.get(0) == binaryType.getLength();
        } else if (logicalType.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
            VarCharType varcharType = (VarCharType) logicalType;
            return shape.size() == 1 && shape.get(0) <= varcharType.getLength();
        } else if (logicalType.getTypeRoot() == LogicalTypeRoot.VARBINARY) {
            VarBinaryType varbinaryType = (VarBinaryType) logicalType;
            return shape.size() == 1 && shape.get(0) <= varbinaryType.getLength();
        } else {
            // Scalars can either be represented by [1] or an empty shape.
            // This is to allow correct matching of the last dimension of an array.
            return shape.size() == 0 || (shape.size() == 1 && shape.get(0) == 1);
        }
    }

    /** Decrypt API key or other secrets. */
    public static String decryptSecretWithComputePoolKey(
            byte[] secret, CredentialDecrypter decrypter) {
        return new String(decrypter.decrypt(secret), StandardCharsets.UTF_8);
    }

    public static String decryptSecretWithComputePoolKey(
            String secret, CredentialDecrypter decrypter) {
        final byte[] decodedUserSecret = Base64.getDecoder().decode(secret);
        return decryptSecretWithComputePoolKey(decodedUserSecret, decrypter);
    }

    public static String decryptWithComputePoolSecret(byte[] secret) {
        return decryptSecretWithComputePoolKey(secret, InMemoryCredentialDecrypterImpl.INSTANCE);
    }

    public static byte[] signData(String data) {
        return signData(data, InMemoryCredentialDecrypterImpl.INSTANCE);
    }

    public static byte[] signData(String data, CredentialDecrypter decrypter) {
        return decrypter.sign(data.getBytes(StandardCharsets.UTF_8));
    }
}
