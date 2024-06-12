/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.converters.json;

import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ObjectSchema.Builder;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Utility class for sharing methods needed for handling {@link CombinedSchema} both when converting
 * schemas and data.
 */
public class CombinedSchemaUtils {

    /**
     * Transforms a combinedSchema to a simpler schema that directly corresponds to Flink's {@link
     * org.apache.flink.table.types.logical.LogicalType}.
     */
    public static Schema simplifyAllOfSchema(CombinedSchema combinedSchema) {
        ConstSchema constSchema = null;
        EnumSchema enumSchema = null;
        NumberSchema numberSchema = null;
        StringSchema stringSchema = null;
        CombinedSchema combinedSubschema = null;
        Map<String, org.everit.json.schema.Schema> properties = new LinkedHashMap<>();
        Map<String, Boolean> required = new HashMap<>();
        for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
            if (subSchema instanceof ConstSchema) {
                constSchema = (ConstSchema) subSchema;
            } else if (subSchema instanceof EnumSchema) {
                enumSchema = (EnumSchema) subSchema;
            } else if (subSchema instanceof NumberSchema) {
                numberSchema = (NumberSchema) subSchema;
            } else if (subSchema instanceof StringSchema) {
                stringSchema = (StringSchema) subSchema;
            } else if (subSchema instanceof CombinedSchema) {
                combinedSubschema = (CombinedSchema) subSchema;
            }
            CombinedSchemaUtils.collectPropertySchemas(
                    subSchema, properties, required, new HashSet<>());
        }
        if (!properties.isEmpty()) {
            // We combine all properties from all objects/types into a single
            // ObjectSchema. The order of fields in the final RowType follows the
            // rules for fields ordering of an ObjectSchema for such a simplified
            // schema. Therefore `connect.index` of all nested properties will be taken into
            // account first and then properties are sorted alphabetically.
            // see resources/schema/json/all-of-indexing.json
            final Builder builder = ObjectSchema.builder();
            properties.forEach(builder::addPropertySchema);
            required.entrySet().stream()
                    .filter(Entry::getValue)
                    .forEach(e -> builder.addRequiredProperty(e.getKey()));
            return builder.build();
        } else if (combinedSubschema != null) {
            // Any combined subschema takes precedence over primitive subschemas
            return combinedSubschema;
        } else if (constSchema != null) {
            if (stringSchema != null) {
                // Ignore the const, return the string
                return stringSchema;
            } else if (numberSchema != null) {
                // Ignore the const, return the number or integer
                return numberSchema;
            }
        } else if (enumSchema != null) {
            if (stringSchema != null) {
                // Return a string enum
                return stringSchema;
            } else if (numberSchema != null) {
                // Ignore the enum, return the number or integer
                return numberSchema;
            }
        } else if (stringSchema != null && stringSchema.getFormatValidator() != null) {
            if (numberSchema != null) {
                // This is a number or integer with a format
                return numberSchema;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported criterion "
                        + combinedSchema.getCriterion()
                        + " for "
                        + combinedSchema);
    }

    private static void collectPropertySchemas(
            org.everit.json.schema.Schema schema,
            Map<String, Schema> properties,
            Map<String, Boolean> required,
            Set<String> visited) {
        if (visited.contains(schema.toString())) {
            return;
        } else {
            visited.add(schema.toString());
        }
        if (schema instanceof CombinedSchema) {
            CombinedSchema combinedSchema = (CombinedSchema) schema;
            if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
                for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
                    collectPropertySchemas(subSchema, properties, required, visited);
                }
            }
        } else if (schema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) schema;
            for (Map.Entry<String, org.everit.json.schema.Schema> entry :
                    objectSchema.getPropertySchemas().entrySet()) {
                String fieldName = entry.getKey();
                properties.put(fieldName, entry.getValue());
                required.put(fieldName, objectSchema.getRequiredProperties().contains(fieldName));
            }
        } else if (schema instanceof ReferenceSchema) {
            ReferenceSchema refSchema = (ReferenceSchema) schema;
            collectPropertySchemas(refSchema.getReferredSchema(), properties, required, visited);
        }
    }
}
