/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.formats.avro.converters;

import org.apache.flink.util.TestLoggerExtension;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroToFlinkSchemaConverter}. */
@ExtendWith(TestLoggerExtension.class)
public class AvroToFlinkSchemaConverterTest {

    @Test
    void testRecordWithCycles() {
        String avroSchemaString =
                "{\"type\": \"record\",\"name\": \"linked_list\",\"fields\" : "
                        + "[{\"name\": \"value\", \"type\": \"long\"},"
                        + "{\"name\": \"next\", \"type\": [\"null\", \"linked_list\"]}]}";
        final Schema schema = new Parser().parse(avroSchemaString);

        assertThatThrownBy(() -> AvroToFlinkSchemaConverter.toFlinkSchema(schema))
                .hasMessage("Cyclic schemas are not supported.");
    }
}
