/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

import org.apache.flink.annotation.Confluent;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
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
public class MLEvaluateAllFunctionTest {
    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    private TableEnvironment tableEnv;
    private ResolvedCatalogModel testClassificationModelV1;
    private ResolvedCatalogModel testClassificationModelV2;

    private ResolvedCatalogModel testRegressionModelV1;
    private ResolvedCatalogModel testRegressionModelV2;

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
        final Column numericalLabel = Column.physical("numerical_label", DataTypes.DOUBLE());
        final Column categoricalLabel = Column.physical("categorical_label", DataTypes.STRING());
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "test_source");
        options.put("version", "12");
        final ResolvedSchema inputSchema = ResolvedSchema.of(f1, f2);
        final ResolvedSchema classificationOutputSchema = ResolvedSchema.of(categoricalLabel);
        final ResolvedSchema regressionOutputSchema = ResolvedSchema.of(numericalLabel);
        testClassificationModelV1 =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                Schema.newBuilder()
                                        .column("f1", "INT")
                                        .column("f2", "STRING")
                                        .build(),
                                Schema.newBuilder().column("categorical_label", "STRING").build(),
                                ImmutableMap.of("provider", "openai", "task", "CLASSIFICATION"),
                                ""),
                        inputSchema,
                        classificationOutputSchema);
        testClassificationModelV2 =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                Schema.newBuilder()
                                        .column("f1", "INT")
                                        .column("f2", "STRING")
                                        .build(),
                                Schema.newBuilder().column("categorical_label", "STRING").build(),
                                ImmutableMap.of("provider", "vertexai", "task", "CLASSIFICATION"),
                                ""),
                        inputSchema,
                        classificationOutputSchema);
        testRegressionModelV1 =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                Schema.newBuilder()
                                        .column("f1", "INT")
                                        .column("f2", "STRING")
                                        .build(),
                                Schema.newBuilder().column("numerical_label", "DOUBLE").build(),
                                ImmutableMap.of("provider", "openai", "task", "REGRESSION"),
                                ""),
                        inputSchema,
                        regressionOutputSchema);
        testRegressionModelV2 =
                ResolvedCatalogModel.of(
                        CatalogModel.of(
                                Schema.newBuilder()
                                        .column("f1", "INT")
                                        .column("f2", "STRING")
                                        .build(),
                                Schema.newBuilder().column("numerical_label", "DOUBLE").build(),
                                ImmutableMap.of("provider", "vertexai", "task", "REGRESSION"),
                                ""),
                        inputSchema,
                        regressionOutputSchema);
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(f1, f2, numericalLabel, categoricalLabel);
        final CatalogBaseTable testTable =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(tableSchema).build(),
                                "",
                                Collections.emptyList(),
                                options),
                        tableSchema);
        try {
            CatalogDatabase db =
                    new CatalogDatabaseImpl(Collections.singletonMap("key1", "val1"), "");
            tableEnv.getCatalog("ml_catalog").get().createDatabase("db", db, false);
            tableEnv.getCatalog("ml_catalog")
                    .get()
                    .createTable(ObjectPath.fromString("db.my_table"), testTable, false);
            tableEnv.getCatalog("ml_catalog")
                    .get()
                    .createModel(
                            ObjectPath.fromString("db.classification_model"),
                            testClassificationModelV1,
                            true);
            tableEnv.getCatalog("ml_catalog")
                    .get()
                    .createModel(
                            ObjectPath.fromString("db.classification_model"),
                            testClassificationModelV2,
                            true);
            tableEnv.getCatalog("ml_catalog")
                    .get()
                    .createModel(
                            ObjectPath.fromString("db.regression_model"),
                            testRegressionModelV1,
                            true);
            tableEnv.getCatalog("ml_catalog")
                    .get()
                    .createModel(
                            ObjectPath.fromString("db.regression_model"),
                            testRegressionModelV2,
                            true);
            tableEnv.loadModule("ml", new MLFunctionsModule());
        } catch (Exception e) {
            // ignore
            throw new RuntimeException(e);
        }
        INSTANCE.configureEnvironment(
                tableEnv, Collections.emptyMap(), Collections.emptyMap(), Service.SQL_SERVICE);
    }

    @Test
    void testMLEvaluateAllClassificationModel() throws Exception {
        final String functionName = "ml_catalog.db.classification_model_ML_EVALUATE_ALL";
        Map<String, Tuple2<Boolean, Map<String, String>>> serializedModelAllVersions =
                new HashMap<>();
        serializedModelAllVersions.put(
                "001", Tuple2.of(true, testClassificationModelV1.toProperties()));
        serializedModelAllVersions.put(
                "002", Tuple2.of(false, testClassificationModelV2.toProperties()));
        tableEnv.createTemporaryFunction(
                functionName,
                new MLEvaluateAllFunction(
                        functionName,
                        new ModelVersions(
                                "ml_catalog.db.classification_model", serializedModelAllVersions)));
        final TableResult mlQueryResult1 =
                tableEnv.executeSql(
                        "SELECT ml_catalog.db.classification_model_ML_EVALUATE_ALL('ml_catalog.db.classification_model$all', f1, f2, categorical_label) AS metrics FROM ml_catalog.db.my_table\n;");
        assertThat(mlQueryResult1.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical(
                                        "metrics",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "VersionId", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "IsDefaultVersion",
                                                                DataTypes.BOOLEAN()),
                                                        DataTypes.FIELD(
                                                                "ACCURACY", DataTypes.DOUBLE()),
                                                        DataTypes.FIELD(
                                                                "PRECISION", DataTypes.DOUBLE()),
                                                        DataTypes.FIELD(
                                                                "RECALL", DataTypes.DOUBLE()),
                                                        DataTypes.FIELD(
                                                                "F1", DataTypes.DOUBLE()))))));
    }

    @Test
    void testMLEvaluateAllRegressionModel() throws Exception {
        final String functionName = "ml_catalog.db.regression_model_ML_EVALUATE_ALL";
        Map<String, Tuple2<Boolean, Map<String, String>>> serializedModelAllVersions =
                new HashMap<>();
        serializedModelAllVersions.put(
                "001", Tuple2.of(true, testRegressionModelV1.toProperties()));
        serializedModelAllVersions.put(
                "002", Tuple2.of(false, testRegressionModelV2.toProperties()));
        tableEnv.createTemporaryFunction(
                functionName,
                new MLEvaluateAllFunction(
                        functionName,
                        new ModelVersions(
                                "ml_catalog.db.regression_model", serializedModelAllVersions)));
        final TableResult mlQueryResult1 =
                tableEnv.executeSql(
                        "SELECT ml_catalog.db.regression_model_ML_EVALUATE_ALL('ml_catalog.db.regression_model$all', f1, f2, numerical_label) AS metrics FROM ml_catalog.db.my_table\n;");
        assertThat(mlQueryResult1.getResolvedSchema())
                .isEqualTo(
                        ResolvedSchema.of(
                                Column.physical(
                                        "metrics",
                                        DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "VersionId", DataTypes.STRING()),
                                                        DataTypes.FIELD(
                                                                "IsDefaultVersion",
                                                                DataTypes.BOOLEAN()),
                                                        DataTypes.FIELD(
                                                                "MEAN_ABSOLUTE_ERROR",
                                                                DataTypes.DOUBLE()),
                                                        DataTypes.FIELD(
                                                                "ROOT_MEAN_SQUARED_ERROR",
                                                                DataTypes.DOUBLE()))))));
    }
}
