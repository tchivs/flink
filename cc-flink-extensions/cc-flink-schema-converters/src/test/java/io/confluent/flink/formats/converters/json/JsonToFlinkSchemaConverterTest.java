/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLoggerExtension;

import io.confluent.flink.formats.converters.json.CommonMappings.TypeMapping;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.loader.SchemaLoader;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JsonToFlinkSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
public class JsonToFlinkSchemaConverterTest {

    public static Stream<Arguments> typesToCheck() throws IOException {
        return Stream.concat(
                        CommonMappings.get(),
                        Stream.of(
                                unionDifferentStruct(),
                                combinedSchemaAllOfRef(),
                                complexAuditLoggingSchema()))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typesToCheck")
    void testTypeMapping(TypeMapping mapping) {
        final LogicalType flinkSchema =
                JsonToFlinkSchemaConverter.toFlinkSchema(mapping.getJsonSchema());
        assertThat(flinkSchema).isEqualTo(mapping.getFlinkType());
    }

    @Test
    void testCyclicDependency() {
        String schemaStr =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"title\": \"IdentifyUserMessage\",\n"
                        + "  \"type\": \"object\",\n"
                        + "  \"$defs\": {\n"
                        + "    \"MessageBase\": {\n"
                        + "      \"properties\": {\n"
                        + "        \"id\": {\n"
                        + "          \"type\": \"string\"\n"
                        + "        },\n"
                        + "\n"
                        + "        \"fallbackMessage\": {\n"
                        + "             \"$ref\": \"#/$defs/MessageBase\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"properties\": {\n"
                        + "    \"msg\": {\n"
                        + "      \"$ref\": \"#/$defs/MessageBase\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "}";
        JSONObject rawSchema = new JSONObject(new JSONTokener(new StringReader(schemaStr)));
        assertThatThrownBy(
                        () ->
                                JsonToFlinkSchemaConverter.toFlinkSchema(
                                        SchemaLoader.load(rawSchema)))
                .isInstanceOf(ValidationException.class)
                .hasMessage("Cyclic schemas are not supported.");
    }

    @Test
    void testJsonTuples() {
        String schemaStr =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"title\": \"IdentifyUserMessage\",\n"
                        + "  \"type\": \"object\",\n"
                        + "  \"properties\": {\n"
                        + "    \"msg\": {\n"
                        + "      \"type\": \"array\",\n"
                        + "      \"items\": [{\"type\": \"string\"}]\n"
                        + "    }\n"
                        + "  },\n"
                        + "}";
        JSONObject rawSchema = new JSONObject(new JSONTokener(new StringReader(schemaStr)));
        assertThatThrownBy(
                        () ->
                                JsonToFlinkSchemaConverter.toFlinkSchema(
                                        SchemaLoader.load(rawSchema)))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "JSON tuples (i.e. arrays that contain items of different types) are not supported");
    }

    static TypeMapping unionDifferentStruct() {
        final Map<String, Object> numberSchemaProps = new HashMap<>();
        numberSchemaProps.put("connect.index", 0);
        numberSchemaProps.put("connect.type", "int8");
        NumberSchema numberSchema =
                NumberSchema.builder().unprocessedProperties(numberSchemaProps).build();
        StringSchema stringSchema =
                StringSchema.builder()
                        .unprocessedProperties(Collections.singletonMap("connect.index", 1))
                        .build();
        ObjectSchema firstSchema =
                ObjectSchema.builder()
                        .addPropertySchema("a", numberSchema)
                        .addPropertySchema("b", stringSchema)
                        .addRequiredProperty("a")
                        .title("field0")
                        .unprocessedProperties(Collections.singletonMap("connect.index", 0))
                        .build();
        ObjectSchema secondSchema =
                ObjectSchema.builder()
                        .addPropertySchema("c", numberSchema)
                        .addPropertySchema("d", stringSchema)
                        .addRequiredProperty("d")
                        .title("field1")
                        .unprocessedProperties(Collections.singletonMap("connect.index", 1))
                        .build();
        return new TypeMapping(
                CombinedSchema.oneOf(Arrays.asList(firstSchema, secondSchema)).build(),
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField(
                                        "connect_union_field_0",
                                        new RowType(
                                                true,
                                                Arrays.asList(
                                                        new RowField("a", new TinyIntType(false)),
                                                        new RowField(
                                                                "b",
                                                                new VarCharType(
                                                                        true,
                                                                        VarCharType.MAX_LENGTH))))),
                                new RowField(
                                        "connect_union_field_1",
                                        new RowType(
                                                true,
                                                Arrays.asList(
                                                        new RowField("c", new TinyIntType(true)),
                                                        new RowField(
                                                                "d",
                                                                new VarCharType(
                                                                        false,
                                                                        VarCharType
                                                                                .MAX_LENGTH))))))));
    }

    static TypeMapping combinedSchemaAllOfRef() {
        String schemaStr =
                "{\n"
                        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                        + "  \"allOf\": [\n"
                        + "    { \"$ref\": \"#/definitions/MessageBase\" },\n"
                        + "    { \"$ref\": \"#/definitions/MessageBase2\" }\n"
                        + "  ],\n"
                        + "\n"
                        + "  \"title\": \"IdentifyUserMessage\",\n"
                        + "  \"description\": \"An IdentifyUser message\",\n"
                        + "  \"type\": \"object\",\n"
                        + "\n"
                        + "  \"properties\": {\n"
                        + "    \"prevUserId\": {\n"
                        + "      \"type\": \"string\"\n"
                        + "    },\n"
                        + "\n"
                        + "    \"newUserId\": {\n"
                        + "      \"type\": \"string\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "\n"
                        + "  \"definitions\": {\n"
                        + "    \"MessageBase\": {\n"
                        + "      \"properties\": {\n"
                        + "        \"id\": {\n"
                        + "          \"type\": \"string\"\n"
                        + "        },\n"
                        + "\n"
                        + "        \"type\": {\n"
                        + "          \"type\": \"string\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    },\n"
                        + "    \"MessageBase2\": {\n"
                        + "      \"properties\": {\n"
                        + "        \"id2\": {\n"
                        + "          \"type\": \"string\"\n"
                        + "        }\n"
                        + "      }\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";
        JSONObject rawSchema = new JSONObject(new JSONTokener(new StringReader(schemaStr)));
        final VarCharType stringType = new VarCharType(VarCharType.MAX_LENGTH);
        return new TypeMapping(
                SchemaLoader.load(rawSchema),
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField("id", stringType),
                                new RowField("type", stringType),
                                new RowField("id2", stringType),
                                new RowField("newUserId", stringType),
                                new RowField("prevUserId", stringType))));
    }

    static TypeMapping complexAuditLoggingSchema() throws IOException {
        final JSONObject rawSchema = readSchemaFromFile("schema/json/complex-audit-logging.json");
        final VarCharType stringType = new VarCharType(VarCharType.MAX_LENGTH);
        final LogicalType addressType = getAddressType(stringType);
        final RowType requestMetadataType =
                new RowType(
                        Arrays.asList(
                                new RowField(
                                        "clientId",
                                        stringType,
                                        "A client-provided identifier, logged for correlation, as a courtesy to the client."),
                                new RowField(
                                        "requestId",
                                        new ArrayType(true, stringType.copy(false)),
                                        "Uniquely identifies a client request."),
                                new RowField(
                                        "clientTraceId",
                                        stringType,
                                        "A client-provided identifier, logged for correlation, as a courtesy to the client."),
                                new RowField(
                                        "connectionId",
                                        stringType,
                                        "Uniquely identifies an authenticated connection. Only present for successfully authenticated connections."),
                                new RowField(
                                        "clientAddress",
                                        new ArrayType(addressType),
                                        "Network address of the remote client.")));
        final RowType emptyRowType = new RowType(Collections.emptyList());
        final RowType resultType =
                new RowType(
                        Arrays.asList(
                                new RowField(
                                        "data",
                                        emptyRowType,
                                        "Audit-worthy details from the operation's result. There are currently no guarantees of backwards-compatibility on the contents of this field. The contents of this field should not be relied on for any programmatic access."),
                                new RowField("status", stringType, "The status of the result.")));
        final RowType requestType =
                new RowType(
                        Arrays.asList(
                                new RowField("accessType", stringType, "The type of the request"),
                                new RowField(
                                        "data",
                                        emptyRowType,
                                        "Audit-worthy details from the request. There are currently no guarantees of backwards-compatibility on the contents of this field. The contents of this field should not be relied on for any programmatic access.")));
        final RowType resourceType =
                new RowType(
                        Collections.singletonList(
                                new RowField("resourceId", stringType, "Resource identifier")));
        final RowType resourceWithTypeType =
                new RowType(
                        Arrays.asList(
                                new RowField("resourceId", stringType, "Resource Identifier"),
                                new RowField("type", stringType)));
        final RowType principalType = getPrincipalType(stringType, resourceType);
        final RowType credentialsType =
                new RowType(
                        Arrays.asList(
                                new RowField(
                                        "idTokenCredentials",
                                        new RowType(
                                                Arrays.asList(
                                                        new RowField(
                                                                "audience",
                                                                new ArrayType(
                                                                        stringType.copy(false))),
                                                        new RowField(
                                                                "subject",
                                                                stringType,
                                                                "Identifies the principal."),
                                                        new RowField(
                                                                "type",
                                                                stringType,
                                                                "The type of the Id Token Credential. JWT for the foreseeable future."),
                                                        new RowField(
                                                                "issuer",
                                                                stringType,
                                                                "Who signed the token.")))),
                                new RowField(
                                        "certificateCredentials",
                                        new RowType(
                                                Collections.singletonList(
                                                        new RowField(
                                                                "dname",
                                                                new RowType(
                                                                        Arrays.asList(
                                                                                new RowField(
                                                                                        "st",
                                                                                        stringType,
                                                                                        "State or province"),
                                                                                new RowField(
                                                                                        "c",
                                                                                        stringType,
                                                                                        "Country"),
                                                                                new RowField(
                                                                                        "ou",
                                                                                        stringType,
                                                                                        "Organizational unit"),
                                                                                new RowField(
                                                                                        "cn",
                                                                                        stringType,
                                                                                        "Common name"),
                                                                                new RowField(
                                                                                        "l",
                                                                                        stringType,
                                                                                        "Locality"),
                                                                                new RowField(
                                                                                        "o",
                                                                                        stringType,
                                                                                        "Organization"))),
                                                                "The principal identified by this certificate.")))),
                                new RowField("mechanism", stringType),
                                new RowField(
                                        "idSecretCredentials",
                                        new RowType(
                                                Collections.singletonList(
                                                        new RowField(
                                                                "credentialId",
                                                                stringType,
                                                                "Identifies the credential"))))));
        final RowType authenticationType =
                new RowType(
                        Arrays.asList(
                                new RowField(
                                        "principal",
                                        principalType,
                                        "Identifies the authenticated principal that is used for any authorization checks. Also identifies the authenticated principal that made the request, unless originalPrincipal is set."),
                                new RowField(
                                        "originalPrincipal",
                                        principalType,
                                        "If set, indicates the original principal that made the request. Used for situations where one principal can assume the identity of a different principal."),
                                new RowField(
                                        "result",
                                        stringType,
                                        "The result of authentication checks on the provided credentials."),
                                new RowField("credentials", credentialsType),
                                new RowField(
                                        "identity",
                                        stringType,
                                        "Identity of the requester in the format."),
                                new RowField(
                                        "errorMessage",
                                        stringType,
                                        "A short, human-readable description of the reason authentication failed.")));
        final RowType aclAuthorizationType =
                new RowType(
                        Arrays.asList(
                                new RowField("permissionType", stringType),
                                new RowField("patternName", stringType),
                                new RowField("host", stringType),
                                new RowField("patternType", stringType),
                                new RowField(
                                        "actingPrincipal",
                                        principalType,
                                        "The decisive principal used for authorization."),
                                new RowField("resourceType", stringType)));
        final RowType rbacAuthorizationType =
                new RowType(
                        Arrays.asList(
                                new RowField("role", stringType),
                                new RowField("patternName", stringType),
                                new RowField(
                                        "cloudScope",
                                        new RowType(
                                                Collections.singletonList(
                                                        new RowField(
                                                                "resources",
                                                                new ArrayType(
                                                                        resourceWithTypeType.copy(
                                                                                false))))),
                                        "The list of cloud resources involved in the scope of an action."),
                                new RowField("patternType", stringType),
                                new RowField(
                                        "actingPrincipal",
                                        principalType,
                                        "The decisive principal used for authorization."),
                                new RowField(
                                        "operation",
                                        stringType,
                                        "The operation which was assigned to the rbac role"),
                                new RowField("resourceType", stringType)));
        final RowType authorizationType =
                new RowType(
                        Arrays.asList(
                                new RowField("result", stringType),
                                new RowField("superUserAuthorization", new BooleanType()),
                                new RowField(
                                        "assignedPrincipals",
                                        new ArrayType(principalType.copy(false))),
                                new RowField("dryRun", new BooleanType()),
                                new RowField("aclAuthorization", aclAuthorizationType),
                                new RowField("resourceName", stringType),
                                new RowField("rbacAuthorization", rbacAuthorizationType),
                                new RowField("operation", stringType),
                                new RowField("resourceType", stringType)));
        final LogicalType cloudResourceType =
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField("resource", resourceWithTypeType),
                                new RowField(
                                        "scope",
                                        new RowType(
                                                Collections.singletonList(
                                                        new RowField(
                                                                "resources",
                                                                new ArrayType(
                                                                        resourceWithTypeType.copy(
                                                                                false)),
                                                                "The list of cloud resources that define the scope of the action."))),
                                        "Defines the scope of the action.")));
        return new TypeMapping(
                SchemaLoader.load(rawSchema),
                new RowType(
                        false,
                        Arrays.asList(
                                new RowField(
                                        "datacontenttype",
                                        stringType,
                                        "Content type of the data value. Adheres to RFC 2046 format."),
                                new RowField(
                                        "data",
                                        new RowType(
                                                Arrays.asList(
                                                        new RowField(
                                                                "requestMetadata",
                                                                requestMetadataType),
                                                        new RowField(
                                                                "result",
                                                                resultType,
                                                                "Describes the result of the overall operation (i.e. success, failure, etc) along with any audit-worthy details. For example, this field contains the resource identifier of a newly created resource from a CREATE request."),
                                                        new RowField(
                                                                "request",
                                                                requestType,
                                                                "Describes additional audit-worthy details about the request."),
                                                        new RowField(
                                                                "authenticationInfo",
                                                                authenticationType,
                                                                "Information about the principal and the credentials used to prove its identity."),
                                                        new RowField(
                                                                "authorizationInfo",
                                                                authorizationType,
                                                                "The result of this authorization check."),
                                                        new RowField(
                                                                "cloudResources",
                                                                new ArrayType(cloudResourceType),
                                                                "The resource(s) or collection(s) targeted in the operation."),
                                                        new RowField(
                                                                "methodName",
                                                                stringType,
                                                                "The type of request being logged."),
                                                        new RowField(
                                                                "resourceName",
                                                                stringType,
                                                                "The resource identifier of the target of the request for compatibility with other request types. Will be deprecated in future."),
                                                        new RowField(
                                                                "serviceName",
                                                                stringType,
                                                                "The resource identifier of the service (the source) that received the request being logged."))),
                                        "Additional details about the audited occurrence."),
                                new RowField(
                                        "subject",
                                        stringType,
                                        "Identifies the resource that would be affected by the event."),
                                new RowField(
                                        "specversion",
                                        stringType.copy(false),
                                        "The version of the CloudEvents specification which the event uses."),
                                new RowField(
                                        "id",
                                        stringType.copy(false),
                                        "Uniquely identifies the event."),
                                new RowField(
                                        "source",
                                        stringType.copy(false),
                                        "Identifies the context in which an event happened."),
                                new RowField(
                                        "time",
                                        stringType,
                                        "Timestamp of when the occurrence happened. Adheres to RFC 3339."),
                                new RowField(
                                        "type",
                                        stringType.copy(false),
                                        "Describes the type of event."),
                                new RowField(
                                        "dataschema",
                                        stringType,
                                        "Identifies the schema that data adheres to. Currently unused."))));
    }

    @NotNull
    private static JSONObject readSchemaFromFile(String resourcePath) throws IOException {
        try (final InputStream resourceAsStream =
                        JsonToFlinkSchemaConverterTest.class
                                .getClassLoader()
                                .getResourceAsStream(resourcePath);
                final InputStreamReader schemaReader = new InputStreamReader(resourceAsStream)) {
            return new JSONObject(new JSONTokener(schemaReader));
        }
    }

    @NotNull
    private static RowType getPrincipalType(VarCharType stringType, RowType resourceType) {
        final RowType externalAccountType =
                new RowType(
                        Arrays.asList(
                                new RowField(
                                        "subject",
                                        stringType,
                                        "Identity of the requesting party, known to the IdP."),
                                new RowField(
                                        "namespace",
                                        new ArrayType(
                                                new RowType(
                                                        false,
                                                        Arrays.asList(
                                                                new RowField("id", stringType),
                                                                new RowField(
                                                                        "type", stringType)))))));
        return new RowType(
                Arrays.asList(
                        new RowField("identityPool", resourceType),
                        new RowField("confluentServiceAccount", resourceType),
                        new RowField("externalAccount", externalAccountType),
                        new RowField("confluentInternal", stringType),
                        new RowField("email", stringType),
                        new RowField("confluentUser", resourceType),
                        new RowField("group", resourceType)));
    }

    @NotNull
    private static LogicalType getAddressType(VarCharType stringType) {
        final LogicalType ipType =
                new RowType(
                        Arrays.asList(
                                new RowField("connect_union_field_0", stringType),
                                new RowField("connect_union_field_1", stringType)));
        return new RowType(
                false,
                Arrays.asList(
                        new RowField("port", new DoubleType(), "Port number, if known."),
                        new RowField("ip", ipType, "IPv4 or IPv6 address.")));
    }
}
