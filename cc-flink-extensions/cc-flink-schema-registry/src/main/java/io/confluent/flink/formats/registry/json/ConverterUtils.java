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
    static Schema extractNonNullableSchema(Schema readSchema) {
        final Schema nonNullableSchema;
        if (readSchema instanceof CombinedSchema
                && ((CombinedSchema) readSchema).getSubschemas().size() == 2) {
            Schema notNullSchema = null;
            for (Schema subSchema : ((CombinedSchema) readSchema).getSubschemas()) {
                if (!subSchema.equals(NullSchema.INSTANCE)) {
                    notNullSchema = subSchema;
                }
            }
            nonNullableSchema = notNullSchema;
        } else {
            nonNullableSchema = readSchema;
        }
        return nonNullableSchema;
    }

    private ConverterUtils() {}
}
