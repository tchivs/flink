/*
 * Copyright 2023 Confluent Inc.
 */

package org.apache.flink.runtime.security.token;

import org.apache.flink.annotation.Confluent;

/** Extends ExceptionThrowingDelegationTokenReceiver. */
@Confluent
public class ExceptionThrowingDelegationTokenReceiverConfluent
        extends ExceptionThrowingDelegationTokenReceiver {

    // Keep a separate call count so as not to affect other tests checking on the super class.
    public static final ThreadLocal<Integer> ON_NEW_TOKEN_OBTAINED_CALL_COUNT =
            ThreadLocal.withInitial(() -> 0);

    @Override
    public String serviceName() {
        return "throw-Confluent";
    }

    public static void reset() {
        ExceptionThrowingDelegationTokenReceiver.reset();
        ON_NEW_TOKEN_OBTAINED_CALL_COUNT.set(0);
    }

    @Override
    public void onNewTokensObtained(byte[] tokens) throws Exception {
        if (throwInUsage.get()) {
            throw new IllegalArgumentException();
        }
        ON_NEW_TOKEN_OBTAINED_CALL_COUNT.set(ON_NEW_TOKEN_OBTAINED_CALL_COUNT.get() + 1);
    }
}
