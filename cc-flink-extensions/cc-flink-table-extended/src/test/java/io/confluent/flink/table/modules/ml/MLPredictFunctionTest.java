/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogDescriptor;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.CatalogModel.ModelTask;
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.confluent.flink.table.service.ServiceTasks;
import io.confluent.flink.table.service.ServiceTasks.Service;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Confluent ML Functions. */
@Confluent
@ExtendWith(TestLoggerExtension.class)
public class MLPredictFunctionTest {
    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    private TableEnvironment tableEnv;
    private ResolvedCatalogModel testModel;

    @BeforeEach
    public void setUp() {
        final CatalogStore catalogStore = new GenericInMemoryCatalogStore();
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().withCatalogStore(catalogStore).build();
        tableEnv = TableEnvironment.create(settings);

        Configuration configuration = new Configuration();
        configuration.setString("type", "generic_in_memory");
        tableEnv.createCatalog("ml_catalog", CatalogDescriptor.of("ml_catalog", configuration));

        final Column f1 = Column.physical("f1", DataTypes.INT());
        final Column f2 = Column.physical("f2", DataTypes.STRING());
        final Column label = Column.physical("label", DataTypes.STRING());
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "test_source");
        options.put("version", "12");
        final ResolvedSchema inputSchema = ResolvedSchema.of(f1, f2);
        final ResolvedSchema outputSchema = ResolvedSchema.of(label);
        testModel =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                Schema.newBuilder()
                                        .column("f1", "INT")
                                        .column("f2", "STRING")
                                        .build(),
                                Schema.newBuilder().column("label", "STRING").build(),
                                ImmutableMap.of(
                                        "provider",
                                        "openai",
                                        "task",
                                        ModelTask.CLASSIFICATION.name()),
                                ""),
                        inputSchema,
                        outputSchema);
        final CatalogBaseTable testTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(inputSchema).build(),
                                "",
                                Collections.emptyList(),
                                options),
                        inputSchema);
        try {
            CatalogDatabase db =
                    new CatalogDatabaseImpl(Collections.singletonMap("key1", "val1"), "");
            tableEnv.getCatalog("ml_catalog").get().createDatabase("db", db, false);
            tableEnv.getCatalog("ml_catalog")
                    .get()
                    .createTable(ObjectPath.fromString("db.my_table"), testTable, false);
            tableEnv.getCatalog("ml_catalog")
                    .get()
                    .createModel(ObjectPath.fromString("db.my_model"), testModel, false);
            tableEnv.loadModule("ml", new MLFunctionsModule());
        } catch (Exception e) {
            // ignore
            throw new RuntimeException(e);
        }
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), Collections.emptyMap(), Service.SQL_SERVICE);
    }

    @Test
    void testMLPredictFunction() throws Exception {
        final TableResult plainQueryResult =
                tableEnv.executeSql("SELECT f1, f2 FROM ml_catalog.db.my_table\n");
        assertThat(plainQueryResult.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("f1", DataTypes.INT()),
                                Column.physical("f2", DataTypes.STRING())));

        // We need to register the temp function first
        final String functionName = "ml_catalog.db.my_model_ML_PREDICT";
        tableEnv.createTemporaryFunction(
                functionName, new MLPredictFunction(functionName, testModel.toProperties()));
        final TableResult mlQueryResult1 =
                tableEnv.executeSql(
                        "SELECT f1, f2, label\n"
                                + "FROM ml_catalog.db.my_table,\n"
                                + "LATERAL TABLE(ml_catalog.db.my_model_ML_PREDICT('ml_catalog.db.my_model', f1, f2))\n;");
        assertThat(mlQueryResult1.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("f1", DataTypes.INT()),
                                Column.physical("f2", DataTypes.STRING()),
                                Column.physical("label", DataTypes.STRING())));

        final TableResult mlQueryResult2 =
                tableEnv.executeSql(
                        "SELECT f1, label AS predicted_label\n"
                                + "FROM ml_catalog.db.my_table,\n"
                                + "LATERAL TABLE(ml_catalog.db.my_model_ML_PREDICT('ml_catalog.db.my_model', f1, f2))\n;");
        assertThat(mlQueryResult2.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical("f1", DataTypes.INT()),
                                Column.physical("predicted_label", DataTypes.STRING())));
    }

    @Test
    void testMLPredictFunctionSerialization() {
        final MLPredictFunction mlPredictFunction =
                new MLPredictFunction("my_model_ml_predict", testModel.toProperties());
        final String encodedString = EncodingUtils.encodeObjectToString(mlPredictFunction);
        final MLPredictFunction decodedMLPredictFunction =
                EncodingUtils.decodeStringToObject(encodedString, MLPredictFunction.class);
        final Map<String, String> decodedSerializedProperties =
                decodedMLPredictFunction.getSerializedModelProperties();
        assertThat(decodedSerializedProperties).isEqualTo(testModel.toProperties());
    }
}
