/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.runtime.failure;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.confluent.flink.runtime.failure.TypeFailureEnricherTableITCase.assertFailureEnricherLabelIsExpectedLabel;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test that the TypeFailureEnricher correctly labels common exceptions in the DataStream API. */
@SuppressWarnings("serial")
public class TypeFailureEnricherStreamITCase extends TestLogger {

    private static final int PARLLELISM = 1;

    @Test
    public void testSerializationSystemErrorLabel() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARLLELISM);

        env.generateSequence(1, 10 * PARLLELISM)
                .map(
                        new MapFunction<Long, ConsumesTooMuchSpanning>() {
                            @Override
                            public ConsumesTooMuchSpanning map(Long value) throws Exception {
                                return new ConsumesTooMuchSpanning();
                            }
                        })
                .rebalance()
                .output(new DiscardingOutputFormat<ConsumesTooMuchSpanning>());

        assertThatThrownBy(() -> env.execute())
                .satisfies(e -> assertFailureEnricherLabelIsExpectedLabel((Exception) e, "SYSTEM"));
    }

    // ------------------------------------------------------------------------
    //  Custom Data Types with broken Serialization Logic
    // ------------------------------------------------------------------------

    /** {@link Value} reading more buffers than written. */
    public static class ConsumesTooMuchSpanning implements Value {

        @Override
        public void write(DataOutputView out) throws IOException {
            byte[] bytes = new byte[22541];
            out.write(bytes);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            byte[] bytes = new byte[32941];
            in.readFully(bytes);
        }
    }
}
