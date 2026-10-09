package com.salesforce.einstein.hierarchy.store;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;

/**
 * Applies the schema to one shard primary with Flyway. Every shard is migrated separately at startup, the way an
 * operator rolls a schema change shard by shard. Replicas get it through replication.
 */
public final class JdbcSchema {
    private JdbcSchema() {
    }

    public static void migrate(DataSource ds) {
        Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration/postgresql")
                .load()
                .migrate();
    }
}
