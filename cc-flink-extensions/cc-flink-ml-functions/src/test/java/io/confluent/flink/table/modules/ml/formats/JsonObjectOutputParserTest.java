/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import io.confluent.flink.table.utils.mlutils.MlUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JsonObjectOutputParser}. */
public class JsonObjectOutputParserTest {
    @Test
    void testParseResponse() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        JsonObjectOutputParser parser = new JsonObjectOutputParser(outputSchema.getColumns());
        String response = "{\"output\":\"output-text\"}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseResponseArray() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<INT>").build();
        JsonObjectOutputParser parser = new JsonObjectOutputParser(outputSchema.getColumns());
        String response = "{\"output\":[1,2,3]}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[] {1, 2, 3});
        // Array with one extra nesting level should work.
        response = "{\"output\":[[1,2,3]]}";
        row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[] {1, 2, 3});
        // Array with two extra nesting levels should throw.
        final String tooDeep = "{\"output\":[[[1,2,3]]]}";
        assertThatThrownBy(() -> parser.parse(MlUtils.makeResponse(tooDeep)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Error deserializing ML Prediction response field: output: ML Predict attempted to deserialize an nested array of depth 1 from a json array of depth 3");
        // Non-array should throw.
        final String nonArray = "{\"output\":1}";
        assertThatThrownBy(() -> parser.parse(MlUtils.makeResponse(nonArray)))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining(
                        "Error deserializing ML Prediction response field: output: ML Predict attempted to deserialize an array from a non-array json object");
    }

    @Test
    void testParseResponseNestedArray() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<ARRAY<INT>>").build();
        JsonObjectOutputParser parser = new JsonObjectOutputParser(outputSchema.getColumns());
        String response = "{\"output\":[[1,2,3],[4,5,6]]}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[][] {{1, 2, 3}, {4, 5, 6}});
    }

    @Test
    void testParseResponseWithWrapper() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "STRING").build();
        JsonObjectOutputParser parser =
                new JsonObjectOutputParser(outputSchema.getColumns(), "wrapper");
        String response = "{\"wrapper\":{\"output\":\"output-text\"}, \"output\":\"other-text\"}";
        assertThat(parser.parse(MlUtils.makeResponse(response)).toString())
                .isEqualTo("+I[output-text]");
    }

    @Test
    void testParseResponseRecurse() throws Exception {
        Schema outputSchema =
                Schema.newBuilder()
                        .column("output", "STRING")
                        .column("output2", "INT")
                        .column("output3", "DOUBLE")
                        .build();
        JsonObjectOutputParser parser =
                new JsonObjectOutputParser(outputSchema.getColumns(), "wrapper");
        String response =
                "{\"wrapper\":{\"wrong\":\"output-text\", \"recurse\":{\"output\":\"correct-text\"},"
                        + " \"output2\": 2, \"more\": {\"output3\": 3.0}}, \"output\":\"other-text\"}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(3);
        assertThat(row.getField(0).toString()).isEqualTo("correct-text");
        assertThat(row.getField(1)).isEqualTo(2);
        assertThat(row.getField(2)).isEqualTo(3.0);
    }

    @Test
    void testParseResponseSingleOutput() throws Exception {
        Schema outputSchema = Schema.newBuilder().column("output", "ARRAY<INT>").build();
        JsonObjectOutputParser parser =
                new JsonObjectOutputParser(outputSchema.getColumns(), "outputs");
        String response = "{\"outputs\":[[1,2,3]]}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        assertThat(row.getArity()).isEqualTo(1);
        assertThat(row.getField(0)).isEqualTo(new Integer[] {1, 2, 3});
    }

    @Test
    void testParseResponseMultiOutput() throws Exception {
        Schema outputSchema =
                Schema.newBuilder()
                        .column("output", "STRING")
                        .column("output2", "INT")
                        .column("output3", "DOUBLE")
                        .column("output4", "TINYINT")
                        .column("output5", "SMALLINT")
                        .column("output6", "BOOLEAN")
                        .column("output8", "BIGINT")
                        .column("output9", "FLOAT")
                        .column("output10", "CHAR")
                        .column("output11", "VARCHAR")
                        .column("output12", "BINARY")
                        .column("output13", "VARBINARY")
                        .column("output14", "DECIMAL(2,1)")
                        .column("output15", "ARRAY<STRING>")
                        .column("output16", "ARRAY<ARRAY<INT>>")
                        .column("output17", "ROW(field1 INT, field2 BOOLEAN)")
                        .build();
        JsonObjectOutputParser parser = new JsonObjectOutputParser(outputSchema.getColumns());
        String response =
                "{\"output\":\"output-text-prompt\","
                        + "\"output2\":1,"
                        + "\"output3\":2.0,"
                        + "\"output4\":3,"
                        + "\"output5\":4,"
                        + "\"output6\":true,"
                        + "\"output8\":5,"
                        + "\"output9\":6.0,"
                        + "\"output10\":\"a\","
                        + "\"output11\":\"b\","
                        + "\"output12\":\"Yw==\","
                        + "\"output13\":\"ZA==\","
                        + "\"output14\":7.1,"
                        + "\"output15\":[\"a\",\"b\"],"
                        + "\"output16\":[[1,2,3],[4,5,6]],"
                        + "\"output17\":{\"field1\":12,\"field2\":true}}";
        Row row = parser.parse(MlUtils.makeResponse(response));
        DataType[] outputTypes =
                new DataType[] {
                    DataTypes.STRING(),
                    DataTypes.INT(),
                    DataTypes.DOUBLE(),
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.BOOLEAN(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.CHAR(1),
                    DataTypes.VARCHAR(1),
                    DataTypes.BINARY(1),
                    DataTypes.VARBINARY(1),
                    DataTypes.DECIMAL(2, 1),
                    DataTypes.ARRAY(DataTypes.STRING()),
                    DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                    DataTypes.ROW(
                            DataTypes.FIELD("field1", DataTypes.INT()),
                            DataTypes.FIELD("field2", DataTypes.BOOLEAN()))
                };

        DataFormatConverters.RowConverter converter =
                new DataFormatConverters.RowConverter(outputTypes);
        RowData rowData = converter.toInternal(row);
        assertThat(rowData.getArity()).isEqualTo(16);
        assertThat(rowData.getString(0)).isEqualTo(StringData.fromString("output-text-prompt"));
        assertThat(rowData.getInt(1)).isEqualTo(1);
        assertThat(rowData.getDouble(2)).isEqualTo(2.0);
        assertThat(rowData.getByte(3)).isEqualTo((byte) 3);
        assertThat(rowData.getShort(4)).isEqualTo((short) 4);
        assertThat(rowData.getBoolean(5)).isEqualTo(true);
        assertThat(rowData.getLong(6)).isEqualTo(5L);
        assertThat(rowData.getFloat(7)).isEqualTo(6.0f);
        assertThat(rowData.getString(8).toString()).isEqualTo("a");
        assertThat(rowData.getString(9).toString()).isEqualTo("b");
        assertThat(rowData.getBinary(10)).isEqualTo("c".getBytes());
        assertThat(rowData.getBinary(11)).isEqualTo("d".getBytes());
        assertThat(rowData.getDecimal(12, 2, 1))
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal(7.1), 2, 1));
        ArrayData arrayData = rowData.getArray(13);
        assertThat(arrayData.size()).isEqualTo(2);
        assertThat(arrayData.getString(0).toString()).isEqualTo("a");
        assertThat(arrayData.getString(1).toString()).isEqualTo("b");
        ArrayData nestedArrayData = rowData.getArray(14);
        assertThat(nestedArrayData.size()).isEqualTo(2);
        assertThat(nestedArrayData.getArray(0).getInt(0)).isEqualTo(1);
        assertThat(nestedArrayData.getArray(0).getInt(1)).isEqualTo(2);
        assertThat(nestedArrayData.getArray(0).getInt(2)).isEqualTo(3);
        assertThat(nestedArrayData.getArray(1).getInt(0)).isEqualTo(4);
        assertThat(nestedArrayData.getArray(1).getInt(1)).isEqualTo(5);
        assertThat(nestedArrayData.getArray(1).getInt(2)).isEqualTo(6);
        assertThat(rowData.getRow(15, 2).toString())
                .isEqualTo(GenericRowData.of(12, true).toString());
    }
}
