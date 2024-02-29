/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.remoteudf.mock;

import java.util.Collections;
import java.util.List;

/** Function Wrapper encapsulating names and types. */
public class MockedFunctionWithTypes {
    final String name;
    final List<String[]> argTypes;
    final List<String> returnType;

    public MockedFunctionWithTypes(String name, String[] argTypes, String returnType) {
        this(name, Collections.singletonList(argTypes), Collections.singletonList(returnType));
    }

    public MockedFunctionWithTypes(String name, List<String[]> argTypes, List<String> returnType) {
        this.name = name;
        this.argTypes = argTypes;
        this.returnType = returnType;
    }

    public String getName() {
        return name;
    }

    public List<String[]> getArgTypes() {
        return argTypes;
    }

    public List<String> getReturnType() {
        return returnType;
    }
}
