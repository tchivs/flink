/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.search.formats;

import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.MissingNode;

import io.confluent.flink.table.modules.ml.formats.DataSerializer;
import io.confluent.flink.table.modules.ml.formats.OutputParser;
import io.confluent.flink.table.utils.RemoteRuntimeUtils;
import okhttp3.Response;

import java.io.IOException;
import java.util.List;

/** Output parser for elastic search. */
public class ElasticOutputParser implements OutputParser {
    private final List<Schema.UnresolvedColumn> outputColumns;
    private final DataSerializer.OutputDeserializer[] outConverters;
    private int embeddingColumnIndex = -1;
    private transient ObjectMapper mapper = new ObjectMapper();

    public ElasticOutputParser(
            List<Schema.UnresolvedColumn> outputColumns, String embeddingColumnName) {
        this.outputColumns = outputColumns;
        outConverters = new DataSerializer.OutputDeserializer[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            if (outputColumns.get(i).getName().equals(embeddingColumnName)) {
                // Note that elastic output never actually has the embedding column, we just
                // need to know where to put the nulls.
                embeddingColumnIndex = i;
            }
            outConverters[i] =
                    DataSerializer.getDeserializer(
                            RemoteRuntimeUtils.getLogicalType(outputColumns.get(i)));
        }
    }

    @Override
    public Row parse(Response response) {
        final String responseString = RemoteRuntimeUtils.getResponseString(response);
        final JsonNode jsonNode;
        try {
            jsonNode = mapper.readTree(responseString);
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Error parsing Elastic response: response was not valid json") {};
        }
        // Elastic API Reference:
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html#search-api-knn
        final JsonNode hits = jsonNode.at("/hits/hits");
        if (hits == null || hits instanceof MissingNode) {
            throw new FlinkRuntimeException("No '/hits/hits' field found in Elastic response.");
        }
        if (!hits.isArray()) {
            throw new FlinkRuntimeException("Elastic 'hits' field is not an array.");
        }

        Row[] rowValues = new Row[hits.size()];
        for (int i = 0; i < hits.size(); i++) {
            rowValues[i] = Row.withPositions(outputColumns.size());
            JsonNode match = hits.get(i);
            if (!match.has("_source")) {
                throw new FlinkRuntimeException(
                        "Elastic 'hits' array contained a match without a '_source' field.");
            }
            // TODO: Do we want the scores, too?
            JsonNode source = match.get("_source");
            try {
                for (int j = 0; j < outputColumns.size(); j++) {
                    if (j == embeddingColumnIndex) {
                        // Elastic doesn't return the embedding column, so we just set it to null.
                        rowValues[i].setField(j, null);
                    } else {
                        JsonNode col = source.get(outputColumns.get(j).getName());
                        if (col == null) {
                            throw new FlinkRuntimeException(
                                    "Elastic '_source' field did not contain a field named '"
                                            + outputColumns.get(j).getName()
                                            + "' for hit index "
                                            + i);
                        }
                        rowValues[i].setField(j, outConverters[j].convert(col));
                    }
                }
            } catch (IOException e) {
                throw new FlinkRuntimeException(
                        "Error parsing Elastic response: " + e.getMessage());
            }
        }

        Row row = Row.of(rowValues);
        return row;
    }
}
