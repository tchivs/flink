/*
 * Copyright 2024 Confluent Inc.
 */

package io.confluent.flink.table.modules.ml;

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
import org.apache.flink.table.catalog.CatalogStore;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStore;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;

import io.confluent.flink.table.service.ServiceTasks;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class MLMinMaxScalarFunctionTest {
    private static final ServiceTasks INSTANCE = ServiceTasks.INSTANCE;

    private TableEnvironment tableEnv;

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
        final Map<String, String> options = new HashMap<>();
        options.put("connector", "test_source");
        options.put("version", "12");
        final ResolvedSchema tableSchema =
                ResolvedSchema.of(f1, f2);
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
            tableEnv.loadModule("ml", new MLFunctionsModule());
        } catch (Exception e) {
            // ignore
            throw new RuntimeException(e);
        }
        INSTANCE.configureEnvironment(
                tableEnv,
                Collections.emptyMap(),
                Collections.emptyMap(),
                ServiceTasks.Service.SQL_SERVICE);
    }

    @Test
    void testMLMinMaxScalarForIntValues() throws Exception {
        // Registering temp function
        final String functionName = "ml_catalog.db.ML_MIN_MAX_SCALAR";
        tableEnv.createTemporaryFunction(functionName, new MLMinMaxScalarFunction(functionName));
        final TableResult mlqueryResult1 =
                tableEnv.executeSql(
                        "SELECT ml_catalog.db.ML_MIN_MAX_SCALAR(1,1,5) AS scaled_value FROM ml_catalog.db.my_table\n;");
        Row row = mlqueryResult1.collect().next();
        assertThat(row.getField(0)).isEqualTo(0.2);
    }
}
