/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.annotation.Experimental;

/**
 * Checked exception that can be thrown to indicate exceptions through illegal inputs from the user
 * to differentiate user-caused errors from system errors. For example when the a configured value
 * provided by the user is not allowed for the options key. This exception remains unchecked on
 * purpose.
 */
@Experimental
public class IllegalUserInputException extends FlinkRuntimeException {

    private static final long serialVersionUID = 1L;

    public IllegalUserInputException(String message) {
        super(message);
    }

    public IllegalUserInputException(Throwable cause) {
        super(cause);
    }

    public IllegalUserInputException(String message, Throwable cause) {
        super(message, cause);
    }
}
