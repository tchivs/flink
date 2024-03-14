/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.flink.table.service;

import org.apache.flink.annotation.Confluent;

/**
 * Official list of supported SQL statements in Confluent Cloud.
 *
 * <p>This list is public facing and ends up in Statement API.
 */
@Confluent
public enum SqlStatementKind {
    // -------------- DDL --------------
    CREATE_TABLE(Category.DDL),
    ALTER_TABLE(Category.DDL),
    CREATE_FUNCTION(Category.DDL),
    DROP_FUNCTION(Category.DDL),
    // -------------- DQL --------------
    DESCRIBE(Category.DQL),
    SELECT(Category.DQL),
    SHOW_CATALOGS(Category.DQL),
    SHOW_CURRENT_CATALOG(Category.DQL),
    SHOW_DATABASES(Category.DQL),
    SHOW_CURRENT_DATABASE(Category.DQL),
    SHOW_TABLES(Category.DQL),
    SHOW_CREATE_TABLE(Category.DQL),
    SHOW_FUNCTIONS(Category.DQL),
    SHOW_JOBS(Category.DQL),
    // -------------- DML --------------
    INSERT_INTO(Category.DML),
    EXECUTE_STATEMENT_SET(Category.DML);

    private final Category category;

    SqlStatementKind(Category category) {
        this.category = category;
    }

    public Category getCategory() {
        return category;
    }

    /** Official categorization of SQL statements in Confluent Cloud. */
    public enum Category {
        DDL,
        DQL,
        DML
    }
}
