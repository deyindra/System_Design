package com.salesforce.einstein.tagging.store;

import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.util.UUID;

/** A fresh, migrated, in-memory H2 database in PostgreSQL mode. */
public final class H2 {
    private H2() {
    }

    public static DataSource newDatabase() {
        DataSource ds = empty();
        JdbcSchema.migrate(ds);
        return ds;
    }

    /** A fresh database with the trending projection's schema. */
    public static DataSource newTrendDatabase() {
        DataSource ds = empty();
        JdbcSchema.migrateTrends(ds);
        return ds;
    }

    private static DataSource empty() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID()
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1");
        return ds;
    }
}
