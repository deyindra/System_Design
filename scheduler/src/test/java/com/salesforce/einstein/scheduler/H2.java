package com.salesforce.einstein.scheduler;

import com.salesforce.einstein.scheduler.store.JdbcTaskStore;
import org.h2.jdbcx.JdbcDataSource;

import javax.sql.DataSource;
import java.util.UUID;

/** Test helper: a fresh, private in-memory H2 database per call, shared by every store built on it. */
public final class H2 {
    private H2() {}

    public static DataSource freshDatabase() {
        JdbcDataSource ds = new JdbcDataSource();
        // DB_CLOSE_DELAY=-1 keeps the database alive between connections; LOCK_TIMEOUT lets a claim
        // wait for a competing node's row lock instead of failing at once.
        ds.setURL("jdbc:h2:mem:sched-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000");
        return ds;
    }

    /** One node's store on {@code ds}, schema created. */
    public static JdbcTaskStore store(DataSource ds) {
        return new JdbcTaskStore(ds).createSchema();
    }
}
