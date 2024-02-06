/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.registry.json;

import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.Schema;

/** Utility methods to use in JSON/RowData converters. */
class ConverterUtils {

    /**
     * The most universal way a nullable value can be represented in a JSON schema is through a
     * union with {@code NullSchema}, similarly like AVRO does it.
     *
     * <p>This is:
     *
     * <pre>{@code
     * oneOf {
     *   NullSchema,
     *   NumberSchema (or any other schema)
     * }
     * }</pre>
     *
     * <p>The method extracts the non-null schema.
     */
    static Schema extractNonNullableSchemaFromOneOf(Schema readSchema) {
        final Schema nonNullableSchema;
        if (readSchema instanceof CombinedSchema
                && ((CombinedSchema) readSchema).getSubschemas().size() == 2
                && ((CombinedSchema) readSchema).getCriterion() == CombinedSchema.ONE_CRITERION) {
            Schema notNullSchema = null;
            boolean nullSchemaFound = false;
            for (Schema subSchema : ((CombinedSchema) readSchema).getSubschemas()) {
                if (subSchema.equals(NullSchema.INSTANCE)) {
                    nullSchemaFound = true;
                } else {
                    notNullSchema = subSchema;
                }
            }
            if (nullSchemaFound) {
                nonNullableSchema = notNullSchema;
            } else {
                nonNullableSchema = readSchema;
            }
        } else {
            nonNullableSchema = readSchema;
        }
        return nonNullableSchema;
    }

    private ConverterUtils() {}
}
