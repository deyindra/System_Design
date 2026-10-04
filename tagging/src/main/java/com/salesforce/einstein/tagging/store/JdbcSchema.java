package com.salesforce.einstein.tagging.store;

import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;

/**
 * Applies the schema to one shard with Flyway, picking {@code db/migration/<vendor>} from the
 * database's product name. Every shard is migrated separately and independently at startup, the way
 * an operator rolls a schema change shard by shard.
 */
public final class JdbcSchema {
    private JdbcSchema() {
    }

    public static void migrate(DataSource ds) {
        Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/migration/" + vendor(ds))
                .load()
                .migrate();
    }

    /**
     * Applies the trending projection's schema ({@code db/trends/<vendor>}). It keeps its own history
     * table, so the trend tables may share a database with a shard without the two histories colliding.
     */
    public static void migrateTrends(DataSource ds) {
        Flyway.configure()
                .dataSource(ds)
                .locations("classpath:db/trends/" + vendor(ds))
                .table("trends_schema_history")
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .load()
                .migrate();
    }

    static String vendor(DataSource ds) {
        try (Connection c = ds.getConnection()) {
            String product = c.getMetaData().getDatabaseProductName().toLowerCase(Locale.ROOT);
            if (product.contains("postgres")) {
                return "postgresql";
            }
            if (product.contains("h2")) {
                return "h2";
            }
            throw new IllegalStateException("unsupported database: " + product);
        } catch (SQLException e) {
            throw new IllegalStateException("cannot inspect database", e);
        }
    }
}
