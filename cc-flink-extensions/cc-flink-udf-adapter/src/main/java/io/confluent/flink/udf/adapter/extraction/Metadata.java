/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.udf.adapter.extraction;

import org.apache.flink.table.types.extraction.ConfluentUdfExtractor.Signature;
import org.apache.flink.util.Preconditions;

import java.util.Comparator;
import java.util.List;

/** The metadata extracted from a UDF class. */
public class Metadata {

    private final List<Signature> signatures;
    private final boolean isDeterministic;

    public Metadata(List<Signature> signatures, boolean isDeterministic) {
        Preconditions.checkNotNull(signatures);
        this.signatures = signatures;
        this.signatures.sort(new SignatureComparator());
        this.isDeterministic = isDeterministic;
    }

    public List<Signature> getSignatures() {
        return signatures;
    }

    public boolean isDeterministic() {
        return isDeterministic;
    }

    // Class for ensuring we have a consistent order for signatures. Useful mostly for tests.
    private static class SignatureComparator implements Comparator<Signature> {

        @Override
        public int compare(Signature sig1, Signature sig2) {
            if (sig1.getSerializedArgumentTypes().size()
                    != sig2.getSerializedArgumentTypes().size()) {
                return Integer.compare(
                        sig1.getSerializedArgumentTypes().size(),
                        sig2.getSerializedArgumentTypes().size());
            }
            for (int i = 0; i < sig1.getSerializedArgumentTypes().size(); i++) {
                String arg = sig1.getSerializedArgumentTypes().get(i);
                String otherArg = sig1.getSerializedArgumentTypes().get(i);
                if (arg.equals(otherArg)) {
                    continue;
                }
                return String.CASE_INSENSITIVE_ORDER.compare(arg, otherArg);
            }
            if (sig1.getSerializedReturnType().equals(sig2.getSerializedReturnType())) {
                return 0;
            }
            return String.CASE_INSENSITIVE_ORDER.compare(
                    sig1.getSerializedReturnType(), sig2.getSerializedReturnType());
        }
    }
}
