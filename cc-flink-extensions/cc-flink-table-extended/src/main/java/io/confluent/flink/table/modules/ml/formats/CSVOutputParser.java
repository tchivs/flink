/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvParser;

import io.confluent.flink.table.utils.MlUtils;
import okhttp3.Response;

import java.util.List;

/** Parser for ML_PREDICT model outputs. */
public class CSVOutputParser implements OutputParser {
    private final List<Schema.UnresolvedColumn> outputColumns;
    private final DataSerializer.OutputDeserializer[] outConverters;
    private final LogicalType[] dataTypes;
    private Boolean isSingleArray = false;
    private transient CsvMapper mapper = new CsvMapper();

    public CSVOutputParser(List<Schema.UnresolvedColumn> outputColumns) {
        this.outputColumns = outputColumns;
        outConverters = new DataSerializer.OutputDeserializer[outputColumns.size()];
        dataTypes = new LogicalType[outputColumns.size()];
        if (outputColumns.size() == 1
                && MlUtils.getLogicalType(outputColumns.get(0)).getTypeRoot()
                        == LogicalTypeRoot.ARRAY) {
            // We disallow nested arrays.
            if (MlUtils.getLogicalType(outputColumns.get(0)).getChildren().get(0).getTypeRoot()
                    == LogicalTypeRoot.ARRAY) {
                throw new FlinkRuntimeException(
                        "Error creating ML Predict response parser: "
                                + "Nested arrays are not supported for CSV output.");
            }
            isSingleArray = true;
        }
        for (int i = 0; i < outputColumns.size(); i++) {
            dataTypes[i] = MlUtils.getLogicalType(outputColumns.get(i));
            outConverters[i] = DataSerializer.getDeserializer(dataTypes[i]);
            // We only allow array types if the output is a single array.
            if (!isSingleArray && dataTypes[i].getTypeRoot() == LogicalTypeRoot.ARRAY) {
                throw new FlinkRuntimeException(
                        "Error creating ML Predict response parser: "
                                + "For CSV output, array types are only supported if "
                                + "the output is a single array.");
            }
            // Row types are not supported.
            if (dataTypes[i].getTypeRoot() == LogicalTypeRoot.ROW) {
                throw new FlinkRuntimeException(
                        "Error creating ML Predict response parser: "
                                + "For CSV output, the ROW type is not supported.");
            }
        }
    }

    @Override
    public String acceptedContentTypes() {
        return "text/csv";
    }

    @Override
    public Row parse(Response response) {
        final String responseString = MlUtils.getResponseString(response);
        MappingIterator<List<String>> it;
        try {
            it =
                    mapper.readerForListOf(String.class)
                            .with(CsvParser.Feature.WRAP_AS_ARRAY)
                            .readValues(responseString);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Could not parse ML Predict response as CSV.");
        }

        // We only look at the first row, since we don't support batching.
        List<String> rowList;
        try {
            rowList = it.next();
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Could not parse line 1 of ML Predict response as CSV.");
        }

        if (rowList == null) {
            throw new FlinkRuntimeException("Error parsing ML Predict response: no data found");
        }

        Row row = new Row(outputColumns.size());
        if (isSingleArray) {
            // If the output is a single array, so we assume the entire response is a single array.
            // Our converters are set up to convert the array elements directly.
            try {
                row.setField(0, outConverters[0].convert(rowList));
            } catch (Exception e) {
                throw new FlinkRuntimeException("Error parsing ML Predict response: " + e);
            }
            return row;
        }

        if (rowList.size() != outputColumns.size()) {
            throw new RuntimeException(
                    "Error parsing ML Predict response: expected "
                            + outputColumns.size()
                            + " columns, but found "
                            + rowList.size());
        }

        for (int i = 0; i < outputColumns.size(); i++) {
            try {
                row.setField(i, outConverters[i].convert(rowList.get(i)));
            } catch (Exception e) {
                throw new FlinkRuntimeException("Error parsing ML Predict response: " + e);
            }
        }

        return row;
    }
}
