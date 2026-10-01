package com.salesforce.einstein.scheduler.store;

import com.salesforce.einstein.scheduler.H2;
import com.salesforce.einstein.scheduler.spi.TaskStore;
import com.salesforce.einstein.scheduler.spi.SharedTaskStoreContractTest;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcTaskStoreTest extends SharedTaskStoreContractTest {
    private DataSource ds;

    @Override protected TaskStore newStore() {
        ds = H2.freshDatabase();
        return H2.store(ds);
    }

    /** A second node: its own store instance over the same database. */
    @Override protected TaskStore peerOf(TaskStore store) { return H2.store(ds); }

    @Test
    void schemaCreationIsIdempotentAndPrefixIsValidated() {
        H2.store(ds);                                    // second createSchema on the same database
        assertEquals(0, store.counts().live());
        assertThrows(IllegalArgumentException.class, () -> new JdbcTaskStore(ds, "x; DROP TABLE y"));
    }
}
