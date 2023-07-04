/*
 * Copyright 2022 Confluent Inc.
 */

package io.confluent.flink.common.metrics;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConfluentAdapter}. */
public class ConfluentAdapterTest {
    @Test
    public void testAdaptMetricName() {
        assertThat(ConfluentAdapter.adaptMetricName("someUnchangedMetric"))
                .isEqualTo("someUnchangedMetric");
        assertThat(ConfluentAdapter.adaptMetricName("currentInput3Watermark"))
                .isEqualTo("currentInputNWatermark");
    }

    @Test
    public void testAdaptVariables() {
        Map<String, String> labels = new HashMap<>();
        assertThat(ConfluentAdapter.adaptVariables("someUnchangedMetric", labels)).isEmpty();
        assertThat(ConfluentAdapter.adaptVariables("currentInput14dWatermark", labels)).isEmpty();
        assertThat(ConfluentAdapter.adaptVariables("currentInputFWatermark", labels)).isEmpty();
        assertThat(ConfluentAdapter.adaptVariables("currentInput14Watermark23", labels))
                .containsEntry(ConfluentAdapter.INPUT_INDEX_LABEL_KEY, "14");
        assertThat(ConfluentAdapter.adaptVariables("300currentInput14Watermark23", labels))
                .containsEntry(ConfluentAdapter.INPUT_INDEX_LABEL_KEY, "14");
        assertThat(ConfluentAdapter.adaptVariables("currentInput3Watermark", labels))
                .containsEntry(ConfluentAdapter.INPUT_INDEX_LABEL_KEY, "3");
        labels.put(ConfluentAdapter.TASK_NAME, "output_js_1[437]: Writer");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.TASK_TYPE, ConfluentAdapter.SINK);
        labels.put(ConfluentAdapter.TASK_NAME, "output_js_1[437]: Committer");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.TASK_TYPE, ConfluentAdapter.SINK);

        labels.put(ConfluentAdapter.TASK_NAME, "Sink: jollyFlower");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.TASK_TYPE, ConfluentAdapter.SINK);
        labels.put(ConfluentAdapter.TASK_NAME, "Source: jollyFlower");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.TASK_TYPE, ConfluentAdapter.SOURCE);
        labels.put(ConfluentAdapter.TASK_NAME, "jollyFlower");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.TASK_TYPE, ConfluentAdapter.MIDDLE);

        labels.put(ConfluentAdapter.OPERATOR_NAME, "Sink: jollyFlower");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.OPERATOR_TYPE, ConfluentAdapter.SINK);
        labels.put(ConfluentAdapter.OPERATOR_NAME, "Source: jollyFlower");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.OPERATOR_TYPE, ConfluentAdapter.SOURCE);
        labels.put(ConfluentAdapter.OPERATOR_NAME, "jollyFlower");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.OPERATOR_TYPE, ConfluentAdapter.MIDDLE);

        labels.put(ConfluentAdapter.OPERATOR_NAME, "jollyFlower Sink: ");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.OPERATOR_TYPE, ConfluentAdapter.MIDDLE);
        labels.put(ConfluentAdapter.OPERATOR_NAME, "jollySinkFlower:");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.OPERATOR_TYPE, ConfluentAdapter.MIDDLE);
        labels.put(ConfluentAdapter.OPERATOR_NAME, "Sink: ");
        assertThat(ConfluentAdapter.adaptVariables("someTaskMetric", labels))
                .containsEntry(ConfluentAdapter.OPERATOR_TYPE, ConfluentAdapter.SINK);
    }
}
