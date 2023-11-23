/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.infoschema;

import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectPath;

import io.confluent.flink.table.catalog.CatalogInfo;
import io.confluent.flink.table.catalog.ConfluentCatalogTable;
import io.confluent.flink.table.connectors.InfoSchemaTableFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.confluent.flink.table.infoschema.InfoSchemaTables.DEFINITION_SCHEMA_DATABASE_ID;
import static io.confluent.flink.table.infoschema.InfoSchemaTables.INFORMATION_SCHEMA_DATABASE_ID;
import static io.confluent.flink.table.infoschema.InfoSchemaTables.INFORMATION_SCHEMA_DATABASE_NAME;
import static io.confluent.flink.table.infoschema.InfoSchemaTables.TABLE_CATALOGS;
import static io.confluent.flink.table.infoschema.InfoSchemaTables.TABLE_CATALOG_NAME;
import static io.confluent.flink.table.infoschema.InfoSchemaTables.TABLE_COLUMNS;
import static io.confluent.flink.table.infoschema.InfoSchemaTables.TABLE_SCHEMATA;
import static io.confluent.flink.table.infoschema.InfoSchemaTables.TABLE_TABLES;
import static io.confluent.flink.table.infoschema.InfoSchemaTables.TABLE_TABLE_OPTIONS;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link InfoSchemaTables}. */
public class InfoSchemaTablesTest {

    @Test
    void testListViewsByName() {
        assertThat(InfoSchemaTables.listViewsByName(INFORMATION_SCHEMA_DATABASE_NAME))
                .containsExactlyInAnyOrder(
                        TABLE_CATALOGS,
                        TABLE_CATALOG_NAME,
                        TABLE_SCHEMATA,
                        TABLE_TABLES,
                        TABLE_COLUMNS,
                        TABLE_TABLE_OPTIONS);
    }

    @Test
    void testListViewsById() {
        assertThat(InfoSchemaTables.listViewsById(INFORMATION_SCHEMA_DATABASE_ID))
                .containsExactlyInAnyOrder(
                        TABLE_CATALOGS,
                        TABLE_CATALOG_NAME,
                        TABLE_SCHEMATA,
                        TABLE_TABLES,
                        TABLE_COLUMNS,
                        TABLE_TABLE_OPTIONS);
    }

    @Test
    void testGetViewById() {
        assertThat(
                        InfoSchemaTables.getViewById(
                                CatalogInfo.of("env-123", "cat"),
                                new ObjectPath(INFORMATION_SCHEMA_DATABASE_ID, TABLE_SCHEMATA)))
                .isPresent()
                .map(CatalogView::getExpandedQuery)
                .contains(
                        "SELECT `CATALOG_ID`, `CATALOG_NAME`, `SCHEMA_ID`, `SCHEMA_NAME` "
                                + "FROM `$system`.`$metadata`.`SCHEMATA` "
                                + "WHERE `CATALOG_ID` = 'env-123'");
    }

    @Test
    void testGetViewByName() {
        assertThat(
                        InfoSchemaTables.getViewByName(
                                CatalogInfo.of("env-123", "cat"),
                                new ObjectPath(INFORMATION_SCHEMA_DATABASE_NAME, TABLE_SCHEMATA)))
                .isPresent()
                .map(CatalogView::getExpandedQuery)
                .contains(
                        "SELECT `CATALOG_ID`, `CATALOG_NAME`, `SCHEMA_ID`, `SCHEMA_NAME` "
                                + "FROM `$system`.`$metadata`.`SCHEMATA` "
                                + "WHERE `CATALOG_ID` = 'env-123'");
    }

    @Test
    void testListBaseTablesById() {
        assertThat(InfoSchemaTables.listBaseTablesById(DEFINITION_SCHEMA_DATABASE_ID))
                .containsExactlyInAnyOrder(
                        TABLE_CATALOGS,
                        TABLE_CATALOG_NAME,
                        TABLE_SCHEMATA,
                        TABLE_TABLES,
                        TABLE_COLUMNS,
                        TABLE_TABLE_OPTIONS);
    }

    @Test
    void testBaseTable() {
        assertThat(
                        InfoSchemaTables.getBaseTableById(
                                new ObjectPath(DEFINITION_SCHEMA_DATABASE_ID, TABLE_SCHEMATA)))
                .isPresent()
                .map(ConfluentCatalogTable::getOptions)
                .contains(Collections.singletonMap("connector", InfoSchemaTableFactory.IDENTIFIER));
    }
}
