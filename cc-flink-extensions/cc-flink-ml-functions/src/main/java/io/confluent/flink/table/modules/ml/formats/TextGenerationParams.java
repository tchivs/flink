/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import io.confluent.flink.table.modules.ml.MLModelCommonConstants;
import io.confluent.flink.table.utils.ml.ModelOptionsUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Common parameters for text generation. */
public class TextGenerationParams {
    private static final String PARAMS_PREFIX = MLModelCommonConstants.PARAMS_PREFIX;
    private boolean hasChatParams = false;
    private Map<StandardChatParams, ParameterRef> chatParams = new HashMap<>();
    private Map<SpecialParams, String> specialParams = new HashMap<>();
    private Map<String, Object> allParams =
            new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);

    private class ParameterRef {
        public String name;
        public Object value;

        public ParameterRef(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

    /** Parameters used across most text generation models. */
    public enum StandardChatParams {
        TEMPERATURE,
        TOP_K,
        TOP_P,
        MAX_TOKENS,
        STOP_SEQUENCES
    }

    /**
     * Parameters that are specified at the top level and not inside PARAMS. All of these are
     * currently Strings.
     */
    public enum SpecialParams {
        MODEL_VERSION,
        SYSTEM_PROMPT
    }

    private enum ParamType {
        FLOAT,
        INT,
        STRING,
        BOOL,
        CSV
    }

    private static Map<StandardChatParams, ParamType> standardParamTypes =
            new HashMap() {
                {
                    put(StandardChatParams.TEMPERATURE, ParamType.FLOAT);
                    put(StandardChatParams.TOP_K, ParamType.FLOAT);
                    put(StandardChatParams.TOP_P, ParamType.FLOAT);
                    put(StandardChatParams.MAX_TOKENS, ParamType.INT);
                    put(StandardChatParams.STOP_SEQUENCES, ParamType.CSV);
                }
            };

    // Mapping of standard chat parameters to their possible names in the model options.
    private static Map<StandardChatParams, String[]> standardParamNames =
            new HashMap() {
                {
                    put(StandardChatParams.TEMPERATURE, new String[] {"temperature"});
                    put(
                            StandardChatParams.TOP_K,
                            new String[] {"top_k", "topk", "max_k", "maxk", "k"});
                    put(
                            StandardChatParams.TOP_P,
                            new String[] {"top_p", "topp", "max_p", "maxp", "p"});
                    put(
                            StandardChatParams.MAX_TOKENS,
                            new String[] {
                                "max_tokens",
                                "maxtokens",
                                "maxtokencount",
                                "maxtokenstosample",
                                "maxgenlen",
                                "maxoutputtokens"
                            });
                    put(
                            StandardChatParams.STOP_SEQUENCES,
                            new String[] {"stop_sequences", "stopsequences", "stop"});
                }
            };

    public TextGenerationParams(Map<String, String> modelOptions) {
        ModelOptionsUtils modelOptionsUtil = new ModelOptionsUtils(modelOptions);
        Map<String, String> caseInsensitiveParams =
                modelOptionsUtil.getCaseInsensitiveProviderOptionsStartingWith(PARAMS_PREFIX);
        getStandardParams(caseInsensitiveParams);
        getSpecialParams(modelOptionsUtil, caseInsensitiveParams);
        getAllParams(caseInsensitiveParams);
    }

    public void overrideParam(String param, Object value) {
        // Remove the parameter if it already exists, this ensures that the final param uses
        // the upper/lower casing of the override.
        allParams.remove(param);
        allParams.put(param, value);
    }

    private void getStandardParams(Map<String, String> caseInsensitiveParams) {
        for (StandardChatParams param : standardParamNames.keySet()) {
            // Check all the possible names for the parameter.
            for (String paramName : standardParamNames.get(param)) {
                String typedParamName = paramName + "." + standardParamTypes.get(param);
                if (caseInsensitiveParams.containsKey(paramName)
                        || caseInsensitiveParams.containsKey(typedParamName)) {
                    String lookupParamName =
                            caseInsensitiveParams.containsKey(paramName)
                                    ? paramName
                                    : typedParamName;
                    String paramValue = caseInsensitiveParams.get(lookupParamName);
                    if (paramValue == null) {
                        // This should only happen if the parameter was explicitly set to null.
                        continue;
                    }
                    Object value = null;
                    switch (standardParamTypes.get(param)) {
                        case FLOAT:
                            value = parseDoubleOrNull(paramValue);
                            break;
                        case INT:
                            value = parseLongOrNull(paramValue);
                            break;
                        case STRING:
                            value = caseInsensitiveParams.get(paramValue);
                            break;
                        case CSV:
                            value = Arrays.asList(paramValue.trim().split(","));
                            break;
                        case BOOL:
                            value = parseBooleanOrNull(paramValue);
                            break;
                    }
                    if (value == null) {
                        throw new FlinkRuntimeException(
                                String.format(
                                        "Invalid value for parameter %s: %s",
                                        lookupParamName, paramValue));
                    }
                    chatParams.put(param, new ParameterRef(paramName, value));
                    break;
                }
            }
        }
        if (!chatParams.isEmpty()) {
            hasChatParams = true;
        }
    }

    private void getSpecialParams(
            ModelOptionsUtils modelOptions, Map<String, String> caseInsensitiveParams) {
        for (SpecialParams param : SpecialParams.values()) {
            // First check the top level options.
            String value = modelOptions.getProviderOption(param.toString());
            if (value != null) {
                specialParams.put(param, value);
            }
        }
    }

    private void getAllParams(Map<String, String> caseInsensitiveParams) {
        // Note that we don't remove the StandardChatParams or SpecialParams from the map,
        // so they will be included in allParams.
        for (Map.Entry<String, String> entry : caseInsensitiveParams.entrySet()) {
            // If the key ends in a type, parse it as that type, otherwise infer the type.
            String key = entry.getKey();
            String value = entry.getValue();
            String[] parts = key.split("\\.");
            ParamType type;
            String name = key;
            if (parts.length > 1) {
                try {
                    type = ParamType.valueOf(parts[parts.length - 1].toUpperCase());
                    name = key.substring(0, key.length() - type.toString().length() - 1);
                } catch (IllegalArgumentException e) {
                    type = inferType(value);
                }
            } else {
                type = inferType(value);
            }
            switch (type) {
                case FLOAT:
                    allParams.put(name, parseDoubleOrNull(value));
                    break;
                case INT:
                    allParams.put(name, parseLongOrNull(value));
                    break;
                case STRING:
                    allParams.put(name, value);
                    break;
                case CSV:
                    allParams.put(name, Arrays.asList(value.trim().split(",")));
                    break;
                case BOOL:
                    allParams.put(name, parseBooleanOrNull(value));
                    break;
            }
        }
    }

    private static Double parseDoubleOrNull(String s) {
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLongOrNull(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Boolean parseBooleanOrNull(String s) {
        // return null if it's not "true" or "false" (case-insensitive)
        if (!s.equalsIgnoreCase("true") && !s.equalsIgnoreCase("false")) {
            return null;
        }
        return Boolean.parseBoolean(s);
    }

    private static ParamType inferType(String s) {
        if (parseLongOrNull(s) != null) {
            return ParamType.INT;
        } else if (parseDoubleOrNull(s) != null) {
            return ParamType.FLOAT;
        } else if (parseBooleanOrNull(s) != null) {
            return ParamType.BOOL;
        } else {
            return ParamType.STRING;
        }
        // We never infer CSV type. It must be explicitly specified.
    }

    private void linkUntypedParam(Object value, ObjectNode node, String fieldName) {
        // If the field name has a period, we treat it as a nested field.
        String[] parts = fieldName.split("\\.", -1); // -1 to keep trailing empty strings.
        if (parts.length > 1) {
            // Do a first pass to fix escaped periods.
            for (int i = 1; i < parts.length - 1; i++) {
                if (parts[i].isEmpty()) {
                    parts[i + 1] = parts[i - 1] + "." + parts[i + 1];
                    parts[i - 1] = "";
                }
            }
            int lastNonEmpty =
                    parts[parts.length - 1].isEmpty() ? parts.length - 2 : parts.length - 1;
            // Create the intermediate nodes.
            ObjectNode current = node;
            for (int i = 0; i < lastNonEmpty; i++) {
                if (parts[i].isEmpty()) {
                    continue;
                }
                if (!current.has(parts[i])) {
                    current.putObject(parts[i]);
                }
                current = (ObjectNode) current.get(parts[i]);
            }
            fieldName = parts[lastNonEmpty];
            node = current;
        }
        if (value instanceof Double) {
            node.put(fieldName, (Double) value);
        } else if (value instanceof Long) {
            node.put(fieldName, (Long) value);
        } else if (value instanceof String) {
            node.put(fieldName, (String) value);
        } else if (value instanceof Boolean) {
            node.put(fieldName, (Boolean) value);
        } else if (value instanceof Iterable) {
            for (Object o : (Iterable) value) {
                node.withArray(fieldName).add(o.toString());
            }
        }
    }

    // Link a standard chat parameter to a field in the node, returns the name of the matched
    // parameter.
    public String linkStandardParam(ObjectNode node, String fieldName, StandardChatParams param) {
        if (chatParams.containsKey(param)) {
            ParameterRef ref = chatParams.get(param);
            linkUntypedParam(ref.value, node, fieldName);
            return ref.name;
        }
        return null;
    }

    // Link a parameter from the allParams map to a field in the node, returns the name of the
    // matched parameter.
    public String linkParam(ObjectNode node, String fieldName, String... paramNames) {
        // Check paramNames in order until we find a match.
        for (String paramName : paramNames) {
            if (allParams.containsKey(paramName)) {
                Object value = allParams.get(paramName);
                linkUntypedParam(value, node, fieldName);
                return paramName.toLowerCase();
            }
        }
        return null;
    }

    // Link a parameter from the allParams map to a field in the node, returns the name of the
    // matched parameter.
    public String linkDefaultParam(
            ObjectNode node, String fieldName, Object defaultValue, String... paramNames) {
        // Check paramNames in order until we find a match.
        for (String paramName : paramNames) {
            if (allParams.containsKey(paramName)) {
                Object value = allParams.get(paramName);
                linkUntypedParam(value, node, fieldName);
                return paramName.toLowerCase();
            }
        }
        if (defaultValue != null) {
            linkUntypedParam(defaultValue, node, fieldName);
        }
        return null;
    }

    public void linkAllParamsExcept(ObjectNode node, Set<String> excludedParams) {
        // Remove any null values from the set.
        excludedParams.remove(null);
        // Excluded params should already be lower case, but we'll convert them just in case.
        excludedParams =
                excludedParams.stream().map(String::toLowerCase).collect(Collectors.toSet());
        for (Map.Entry<String, Object> entry : allParams.entrySet()) {
            if (!excludedParams.contains(entry.getKey().toLowerCase())) {
                linkUntypedParam(entry.getValue(), node, entry.getKey());
            }
        }
    }

    public void linkAllParams(ObjectNode node) {
        for (Map.Entry<String, Object> entry : allParams.entrySet()) {
            linkUntypedParam(entry.getValue(), node, entry.getKey());
        }
    }

    public String linkTemperature(ObjectNode node, String fieldName) {
        return linkStandardParam(node, fieldName, StandardChatParams.TEMPERATURE);
    }

    public String linkTopK(ObjectNode node, String fieldName) {
        return linkStandardParam(node, fieldName, StandardChatParams.TOP_K);
    }

    public String linkTopP(ObjectNode node, String fieldName) {
        return linkStandardParam(node, fieldName, StandardChatParams.TOP_P);
    }

    public String linkMaxTokens(ObjectNode node, String fieldName) {
        return linkStandardParam(node, fieldName, StandardChatParams.MAX_TOKENS);
    }

    public String linkStopSequences(ObjectNode node, String fieldName) {
        return linkStandardParam(node, fieldName, StandardChatParams.STOP_SEQUENCES);
    }

    public void linkModelVersion(ObjectNode node, String fieldName, String defaultValue) {
        if (specialParams.containsKey(SpecialParams.MODEL_VERSION)) {
            node.put(fieldName, specialParams.get(SpecialParams.MODEL_VERSION));
        } else if (defaultValue != null) {
            // We allow empty defaults, but not null.
            node.put(fieldName, defaultValue);
        }
    }

    public void linkSystemPrompt(ObjectNode node, String fieldName) {
        if (specialParams.containsKey(SpecialParams.SYSTEM_PROMPT)) {
            node.put(fieldName, specialParams.get(SpecialParams.SYSTEM_PROMPT));
        }
    }

    /**
     * Returns true if the model has any of the standard chat parameters. (Temp, TopK, TopP,
     * maxTokens, or Stop Sequences).
     */
    public boolean hasChatParams() {
        return hasChatParams;
    }

    public String getSystemPrompt() {
        return specialParams.get(SpecialParams.SYSTEM_PROMPT);
    }
}
